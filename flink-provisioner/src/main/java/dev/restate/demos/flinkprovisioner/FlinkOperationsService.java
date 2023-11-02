package dev.restate.demos.flinkprovisioner;

import com.google.protobuf.Empty;
import dev.restate.demos.flinkprovisioner.cloud.FlinkTools;
import dev.restate.demos.flinkprovisioner.cloud.S3;
import dev.restate.demos.flinkprovisioner.util.ProvisionerException;
import dev.restate.demos.flinkprovisioner.util.RestateGrpcChannel;
import dev.restate.sdk.blocking.RestateBlockingService;
import dev.restate.sdk.blocking.RestateContext;
import io.grpc.Status;
import io.grpc.stub.StreamObserver;
import org.restate_demos.flink_provisioner.generated.FlinkClusterGrpc;
import org.restate_demos.flink_provisioner.generated.FlinkOperationsGrpc;
import org.restate_demos.flink_provisioner.generated.FlinkProvisioner;
import org.restate_demos.flink_provisioner.generated.SavepointGrpc;

import java.io.IOException;
import java.util.UUID;

/**
 * The main workflow for forking the cluster via a savepoint.
 */
public class FlinkOperationsService extends FlinkOperationsGrpc.FlinkOperationsImplBase implements RestateBlockingService {

    private static final String RESTATE_URL = "http://localhost:8080";

    @Override
    public void forkFlinkCluster(FlinkProvisioner.ForkFlinkClusterRequest request, StreamObserver<Empty> out) {
        final RestateContext ctx = restateContext();

        final String savepointPath = request.getSavepointPath();
        final String existingFlinkClusterFqdn = request.getFqdn();
        final String newFlinkClusterFqdn = request.getNewFqdn();

        // gRPC clients to talk to the other services
        final var flinkClusterServiceClient = FlinkClusterGrpc.newBlockingStub(new RestateGrpcChannel(ctx));
        final var savepointServiceClient = SavepointGrpc.newBlockingStub(new RestateGrpcChannel(ctx));

        // get details of the Flink cluster. if cluster does not exist,
        // this call throws a terminal exception which we let propagate here
        final var flinkCluster = flinkClusterServiceClient.getFlinkCluster(
                FlinkProvisioner.GetFlinkClusterRequest.newBuilder()
                        .setFqdn(existingFlinkClusterFqdn)
                        .build()
        );

        // reserve the savepoint's path in our savepoint tracker
        final var initSavepointRequest = savepointServiceClient.startSavepoint(
                FlinkProvisioner.StartSavepointRequest.newBuilder()
                        .setPath(savepointPath)
                        .build()
        );
        if (!initSavepointRequest.getValue()) {
            out.onError(new ProvisionerException("Savepoint path already in use"));
            return;
        }

        // ensure S3 bucket exists
        final var bucket = S3
                .verifyUriAndExtractBucketName(savepointPath)
                .orElseThrow(() -> Status.INVALID_ARGUMENT
                    .withCause(new IOException("Invalid s3 Url: " + request.getSavepointPath()))
                    .asRuntimeException());

        // 'ensureBucketExists' methods may fail (mystic AWS error) but the staged retries both in the
        // side effect and the method execution repeat the execution
        ctx.sideEffect(() -> S3.ensureBucketExists(bucket));

        // create an idempotency id for savepoint creation
        // we use the sideEffect to capture a stable id here
        final var savepointRequestDedupId = ctx.sideEffect(String.class, () -> UUID.randomUUID().toString());

        // create the Future that is completed by the external system (savepoint completion)
        final var awakeable = ctx.awakeable(Void.class);

        // create savepoint and register callback
        ctx.sideEffect(() -> FlinkTools.triggerSavepoint(
                flinkCluster.getRestUrl(),
                savepointRequestDedupId,
                savepointPath,
                RESTATE_URL + "/dev.restate.Awakeables/Resolve",
                awakeable.id())
        );

        // wait in the future for the callback from Flink
        awakeable.await();

        // set our savepoint as complete
        savepointServiceClient.completeAndAcquireSavepoint(
                FlinkProvisioner.CompleteAndAcquireSavepointRequest.newBuilder()
                        .setPath(savepointPath)
                        .setClusterFqdn(request.getNewFqdn())
                        .build()
        );

        // launch the new cluster based on that savepoint
        flinkClusterServiceClient.createFlinkCluster(
                FlinkProvisioner.CreateFlinkClusterRequest.newBuilder()
                        .setFqdn(newFlinkClusterFqdn)
                        .setJarPath(request.getJarPath())
                        .setSavepointPath(savepointPath)
                        .build()
        );

        // done!
        out.onNext(Empty.getDefaultInstance());
        out.onCompleted();
    }
}
