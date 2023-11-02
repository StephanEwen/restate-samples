package dev.restate.demos.flinkprovisioner;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.protobuf.Empty;
import dev.restate.demos.flinkprovisioner.cloud.FlinkTools;
import dev.restate.demos.flinkprovisioner.util.ProvisionerException;
import dev.restate.demos.flinkprovisioner.util.RestateGrpcChannel;
import dev.restate.sdk.blocking.RestateBlockingService;
import dev.restate.sdk.blocking.RestateContext;
import dev.restate.sdk.core.StateKey;
import dev.restate.sdk.core.TypeTag;
import dev.restate.sdk.core.serde.jackson.JacksonSerde;
import io.grpc.stub.StreamObserver;
import org.restate_demos.flink_provisioner.generated.FlinkClusterGrpc;
import org.restate_demos.flink_provisioner.generated.FlinkProvisioner;
import org.restate_demos.flink_provisioner.generated.SavepointGrpc;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

/**
 * This class implements a service that is both long-lived stateful (beyond method invocations, like
 * representing the state of a running Flink cluster), but where methods also are simple workflows in
 * themselves (like creating a Flink cluster).
 *
 * This is defined in the proto service spec as a keyed service, where the key is the Flink clusters
 * name (FQDN).
 */
public class FlinkClusterService extends FlinkClusterGrpc.FlinkClusterImplBase implements RestateBlockingService {

    private static final StateKey<String> CREATION_DATE = StateKey.of("created", TypeTag.STRING_UTF8);
    private static final StateKey<String> CLUSTER_URL = StateKey.of("cluster-url", TypeTag.STRING_UTF8);
    private static final StateKey<String> SAVEPOINT_PATH = StateKey.of("savepoint-path", TypeTag.STRING_UTF8);

    @Override
    public void createFlinkCluster(FlinkProvisioner.CreateFlinkClusterRequest request, StreamObserver<Empty> out) {
        final RestateContext ctx = restateContext();
        if (ctx.get(CREATION_DATE).isPresent()) {
            out.onError(new ProvisionerException("Cluster already exists"));
            return;
        }

        final Optional<String> savepoint = request.getSavepointPath().isEmpty()
                ? Optional.empty()
                : Optional.of(request.getSavepointPath());

        // if this starts from a savepoint, acquire a lock/reference first
        if (savepoint.isPresent()) {
            final var savepointServiceClient = SavepointGrpc.newBlockingStub(new RestateGrpcChannel(ctx));
            final boolean acquiredSavepoint = savepointServiceClient.acquireSavepoint(
                    FlinkProvisioner.AcquireSavepointRequest.newBuilder()
                            .setPath(request.getSavepointPath())
                            .setClusterFqdn(request.getFqdn())
                            .build()
                ).getValue();

            if (!acquiredSavepoint) {
                out.onError(new ProvisionerException("Could not acquire savepoint at " + request.getSavepointPath()));
                return;
            }
        }

        final String clusterUrl = FlinkTools.createFlinkCluster(
                ctx,
                request.getFqdn(),
                request.getJarPath(),
                savepoint);

        ctx.set(CREATION_DATE, Instant.now().toString());
        ctx.set(CLUSTER_URL, clusterUrl);

        // if we start from a savepoint, monitor the coming checkpoints to understand when
        // the savepoint is no longer relied upon by the new cluster
        if (savepoint.isPresent()) {
            ctx.set(SAVEPOINT_PATH, savepoint.get());

            // for delayed calls, we currently need to use the GRPC method handles
            // we have custom gRPC code gen in future SDK versions
            ctx.delayedCall(
                    FlinkClusterGrpc.getPollSavepointMethod(),
                    FlinkProvisioner.GetFlinkClusterRequest.newBuilder().setFqdn(request.getFqdn()).build(),
                    Duration.ofSeconds(10));
        }

        out.onNext(Empty.getDefaultInstance());
        out.onCompleted();
    }

    @Override
    public void pollSavepoint(FlinkProvisioner.GetFlinkClusterRequest request, StreamObserver<Empty> out) {
        final RestateContext ctx = restateContext();

        final var url = ctx.get(CLUSTER_URL);
        final var savepoint = ctx.get(SAVEPOINT_PATH);

        // cluster might be gone by now or (someone else polled) not use this savepoint anymore
        if (url.isEmpty() || savepoint.isEmpty()) {
            out.onNext(Empty.getDefaultInstance());
            out.onCompleted();
            return;
        }

        final List<FlinkTools.Checkpoint> checkpointHistory = ctx.sideEffect(
                JacksonSerde.typeRef(new TypeReference<>() {
                }),
                () -> FlinkTools.checkpointHistory(url.get()));

        final boolean found = checkpointHistory.contains(FlinkTools.COMPLETED_CHECKPOINT_TYPE);
        if (found) {
            // remove reference
            ctx.clear(SAVEPOINT_PATH);

            // for send (one way communication) calls, we currently need to use the GRPC method handles
            // we have custom gRPC code gen in future SDK versions
            ctx.oneWayCall(
                    SavepointGrpc.getReleaseSavepointMethod(),
                    FlinkProvisioner.ReleaseSavepointRequest.newBuilder()
                            .setPath(savepoint.get())
                            .setClusterFqdn(request.getFqdn())
                            .build()
            );
        } else {
            // for delayed calls, we currently need to use the GRPC method handles
            // we have custom gRPC code gen in future SDK versions
            ctx.delayedCall(
                    FlinkClusterGrpc.getPollSavepointMethod(),
                    FlinkProvisioner.GetFlinkClusterRequest.newBuilder()
                            .setFqdn(request.getFqdn())
                            .build(),
                    Duration.ofSeconds(10)
            );
        }

        out.onNext(Empty.getDefaultInstance());
        out.onCompleted();
    }

    @Override
    public void getFlinkCluster(FlinkProvisioner.GetFlinkClusterRequest request, StreamObserver<FlinkProvisioner.GetFlinkClusterResponse> out) {
        final RestateContext ctx = restateContext();

        final var created = ctx.get(CREATION_DATE);
        final var restUrl = ctx.get(CLUSTER_URL);
        final var savepoint = ctx.get(SAVEPOINT_PATH);

        if (created.isEmpty() || restUrl.isEmpty()) {
            out.onError(new ProvisionerException("cluster does not exist"));
            return;
        }

        final var response = FlinkProvisioner.GetFlinkClusterResponse.newBuilder();
        response.setCreated(created.get());
        response.setRestUrl(restUrl.get());
        savepoint.ifPresent(response::setSavepointPath);
        out.onNext(response.build());
        out.onCompleted();
    }
}
