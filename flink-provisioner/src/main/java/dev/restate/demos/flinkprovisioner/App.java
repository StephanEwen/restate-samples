package dev.restate.demos.flinkprovisioner;

import dev.restate.sdk.http.vertx.RestateHttpEndpointBuilder;

/**
 * The main entry point, using HTTP/2 (grpc) server. Alternatively, could use the
 * {@link dev.restate.sdk.lambda.LambdaRestateServer} here.
 *
 * Runs all services behind the same endpoint, could also be split up to different endpoints,
 * if desired.
 */
public class App {

    public static void main(String[] args) {
        RestateHttpEndpointBuilder.builder()
                .withService(new SavepointService())
                .withService(new FlinkClusterService())
                .withService(new FlinkOperationsService())
                .buildAndListen(9425);
    }
}
