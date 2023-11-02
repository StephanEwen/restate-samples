package dev.restate.demos.flinkprovisioner.util;

import com.google.protobuf.MessageLite;
import dev.restate.sdk.blocking.Awaitable;
import dev.restate.sdk.blocking.RestateContext;
import io.grpc.*;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * A grpc communication channel that uses Restate to communicate messages.
 * This class will be part of the Restate SDK in the future.
 */
public class RestateGrpcChannel extends Channel {
    private final RestateContext restateContext;

    public RestateGrpcChannel(RestateContext restateContext) {
        this.restateContext = restateContext;
    }

    @Override
    public <RequestT, ResponseT> ClientCall<RequestT, ResponseT> newCall(
            MethodDescriptor<RequestT, ResponseT> methodDescriptor, CallOptions callOptions) {
        return new ClientCall<>() {
            private Listener<ResponseT> responseListener = null;
            private Awaitable<ResponseT> awaitable = null;

            @Override
            public void start(Listener<ResponseT> responseListener, Metadata headers) {
                this.responseListener = responseListener;
            }

            @Override
            public void request(int numMessages) {}

            @Override
            public void cancel(@Nullable String message, @Nullable Throwable cause) {}

            @Override
            public void halfClose() {
                var listener = Objects.requireNonNull(responseListener);
                listener.onHeaders(new Metadata()); // We pass no headers
                try {
                    listener.onMessage(Objects.requireNonNull(this.awaitable).await());
                    listener.onClose(Status.OK, new Metadata());
                } catch (StatusRuntimeException e) {
                    listener.onClose(e.getStatus(), new Metadata());
                }
            }

            // We happily assume RequestT and ResponseT are Protobuf MessageLite implementations
            @SuppressWarnings({"unchecked", "rawtypes"})
            @Override
            public void sendMessage(RequestT message) {
                // We assume only
                this.awaitable =
                        (Awaitable<ResponseT>)
                                restateContext.call((MethodDescriptor) methodDescriptor, (MessageLite) message);
            }
        };
    }

    @Override
    public String authority() {
        return "restate";
    }
}