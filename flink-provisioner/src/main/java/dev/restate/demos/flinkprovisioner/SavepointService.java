package dev.restate.demos.flinkprovisioner;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Empty;
import dev.restate.demos.flinkprovisioner.cloud.S3;
import dev.restate.demos.flinkprovisioner.util.ProvisionerException;
import dev.restate.sdk.blocking.RestateBlockingService;
import dev.restate.sdk.blocking.RestateContext;
import dev.restate.sdk.core.StateKey;
import dev.restate.sdk.core.serde.jackson.JacksonSerde;
import io.grpc.stub.StreamObserver;
import org.restate_demos.flink_provisioner.generated.FlinkProvisioner;
import org.restate_demos.flink_provisioner.generated.SavepointGrpc;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Long-lived stateful entity for savepoints.
 *
 * Tracks status of savepoint (reserved, created, etc.) and the references from Flink clusters.
 *
 * Mostly implements short operations where we care about atomicity of state updates, messages, and
 * RPC life cycle. Some mini-workflows, like when removing the last reference and deleting the savepoint.
 *
 * This is defined in the proto service spec as a keyed service, where the key is the Savepoint path.
 */
public class SavepointService extends SavepointGrpc.SavepointImplBase implements RestateBlockingService {

    private static final StateKey<FlinkProvisioner.SavepointStatus> STATUS =
            StateKey.of("status", FlinkProvisioner.SavepointStatus.class);

    private static final StateKey<Set<String>> REFERENCES =
            StateKey.of("references", JacksonSerde.typeRef(new TypeReference<>() {}));

    @Override
    public void startSavepoint(FlinkProvisioner.StartSavepointRequest request, StreamObserver<BoolValue> out) {
        final RestateContext ctx = restateContext();
        final var status = ctx.get(STATUS).orElse(FlinkProvisioner.SavepointStatus.NOT_FOUND);

        // decline creation if this savepoint path already exists
        if (status != FlinkProvisioner.SavepointStatus.NOT_FOUND) {
            out.onNext(BoolValue.of(false));
            out.onCompleted();
            return;
        }

        ctx.set(STATUS, FlinkProvisioner.SavepointStatus.CREATING);

        out.onNext(BoolValue.of(true));
        out.onCompleted();
    }

    @Override
    public void completeAndAcquireSavepoint(FlinkProvisioner.CompleteAndAcquireSavepointRequest request, StreamObserver<Empty> out) {
        final RestateContext ctx = restateContext();
        final var status = ctx.get(STATUS).orElse(FlinkProvisioner.SavepointStatus.NOT_FOUND);

        if (status != FlinkProvisioner.SavepointStatus.CREATING) {
            out.onError(new ProvisionerException("Invalid state to complete savepoint: " + status));
            // alternative: throw Status.INVALID_ARGUMENT.asRuntimeException();
            return;
        }

        ctx.set(STATUS, FlinkProvisioner.SavepointStatus.LIVE);

        final var references = ctx.get(REFERENCES).orElseGet(HashSet::new);
        references.add(request.getClusterFqdn());
        ctx.set(REFERENCES, references);

        out.onNext(Empty.getDefaultInstance());
        out.onCompleted();
    }

    @Override
    public void acquireSavepoint(FlinkProvisioner.AcquireSavepointRequest request, StreamObserver<BoolValue> out) {
        final RestateContext ctx = restateContext();
        final var status = ctx.get(STATUS).orElse(FlinkProvisioner.SavepointStatus.NOT_FOUND);

        // check if this one is still live
        if (status != FlinkProvisioner.SavepointStatus.LIVE) {
            out.onNext(BoolValue.of(false));
            out.onCompleted();
            return;
        }

        // add a reference
        final var references = ctx.get(REFERENCES).orElseGet(HashSet::new);
        references.add(request.getClusterFqdn());
        ctx.set(REFERENCES, references);

        out.onNext(BoolValue.of(true));
        out.onCompleted();
    }

    @Override
    public void releaseSavepoint(FlinkProvisioner.ReleaseSavepointRequest request, StreamObserver<BoolValue> out) {
        final RestateContext ctx = restateContext();
        final var status = ctx.get(STATUS).orElse(FlinkProvisioner.SavepointStatus.NOT_FOUND);

        // check if this one is still live
        if (status != FlinkProvisioner.SavepointStatus.LIVE) {
            out.onNext(BoolValue.of(false));
            out.onCompleted();
            return;
        }

        // remove a reference
        final var references = ctx.get(REFERENCES).orElseGet(HashSet::new);
        references.remove(request.getClusterFqdn());
        ctx.set(REFERENCES, references);

        // dispose if no more references
        if (references.isEmpty()) {
            ctx.clear(STATUS);
            ctx.clear(REFERENCES);

            ctx.sideEffect(() -> S3.delete(request.getPath()));
        }

        out.onNext(BoolValue.of(references.isEmpty()));
        out.onCompleted();
    }

    @Override
    public void getSavepoint(FlinkProvisioner.GetSavepointRequest request, StreamObserver<FlinkProvisioner.GetSavepointResponse> out) {
        final RestateContext ctx = restateContext();
        final var status = ctx.get(STATUS).orElse(FlinkProvisioner.SavepointStatus.NOT_FOUND);
        final var references = ctx.get(REFERENCES).orElse(Collections.emptySet());

        final var response = FlinkProvisioner.GetSavepointResponse.newBuilder()
                .setStatus(status)
                .addAllClusterReferences(references)
                .build();

        out.onNext(response);
        out.onCompleted();
    }
}
