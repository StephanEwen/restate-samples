package dev.restate.demos.flinkprovisioner.cloud;

import com.fasterxml.jackson.annotation.JsonProperty;
import dev.restate.demos.flinkprovisioner.util.ProvisionerException;
import dev.restate.sdk.blocking.RestateContext;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * This is a mock class for creating and interacting with Flink clusters.
 * It is here, so we don't need actual container runtimes and Flink deployment to run this example.
 *
 * The code simulates that the operations take time, provide async callbacks, and even propagate
 * their "not yet ready" status in a naive way via exceptions.
 */
public final class FlinkTools {

    private static final ScheduledExecutorService DELAY_EXECUTOR = Executors.newSingleThreadScheduledExecutor();

    /**
     * Mock create a Flink cluster.
     * The cluster creation takes some time (20 seconds) and this method repeatedly throws an exception
     * before the cluster is ready and relies on being called again.
     * This simulates the behavior of some of the AWS cloud formation tools.
     */
    public static String createFlinkCluster(
            RestateContext ctx,
            String fqdn,
            String jarPath,
            Optional<String> initialSavepoint) {

        // TMP-fix: we store the instant as a string in this sideEffect to avoid an additional dependency
        //          for serializing Instants. This would change in the future with the new serialization types.
        final String startString = ctx.sideEffect(String.class, () -> Instant.now().toString());
        final Instant start = Instant.parse(startString);

        final Duration elapsedTime = Duration.between(start, Instant.now());
         // after 20 seconds the cluster has been provisioned
         if (elapsedTime.getSeconds() > 20) {
            return "http://localhost:8081";
         } else {
            throw new ProvisionerException("Cluster still creating");
        }
    }


    /**
     * Mocks the triggering of a savepoint.
     * The savepoint takes a while to finish (about 10 secs).
     *
     * Once the savepoint is done, this issues a web-hook-style callback to notify of the completion.
     * This simulates a feature where an external system gives you an async callback on a long-running
     * operation.
     */
    public static void triggerSavepoint(
            final String flinkClusterRestUrl,
            final String savepointId,
            final String targetLocationPath,
            final String callbackUrl,
            final String callbackId) {

        // we don't do anything here, just pretend this works but takes a while
        // we complete the callback after 10 seconds

        final String jsonPayload = "{ \"id\": \"" + callbackId + "\", \"json_result\": {} }";

        final HttpClient client = HttpClient.newHttpClient();
        final HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(callbackUrl))
                .header("Content-Type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(jsonPayload))
                .build();

        final Runnable sender = () -> {
            try {
                final HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
                if (response.statusCode() != 200) {
                    System.err.println("HTTP Request failed: " + response.body());
                }
            } catch (IOException e) {
                System.err.println("HTTP Request failed: " + e.getMessage());
                e.printStackTrace();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                System.err.println("Thread interrupted");
            }
        };

        DELAY_EXECUTOR.schedule(sender, 10, TimeUnit.SECONDS);
    }

    /**
     * Mock REST API call to obtain the checkpoint history from a Flink cluster.
     */
    public static List<Checkpoint> checkpointHistory(String restUrl) {
        boolean completed = COMPLETED.getOrDefault(restUrl, Boolean.FALSE);

        if (!completed) {
            completed = Math.random() < 0.33;
            if (completed) {
                COMPLETED.put(restUrl, true);
            }
        }

        return completed
                ? List.of(INITIAL_SAVEPOINT, COMPLETED_CHECKPOINT_TYPE)
                : List.of(INITIAL_SAVEPOINT);
    }

    public static class Checkpoint {

        public final String type;

        public final String status;

        public Checkpoint(@JsonProperty("type") String type, @JsonProperty("status") String status) {
            Objects.requireNonNull(type);
            Objects.requireNonNull(status);
            this.type = type;
            this.status = status;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Checkpoint that = (Checkpoint) o;
            return Objects.equals(type, that.type) && Objects.equals(status, that.status);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, status);
        }

        @Override
        public String toString() {
            return "Checkpoint{" +
                    "type='" + type + '\'' +
                    ", status='" + status + '\'' +
                    '}';
        }
    }

    private static final Map<String, Boolean> COMPLETED = new HashMap<>();

    private static final Checkpoint INITIAL_SAVEPOINT = new Checkpoint("SAVEPOINT", "INITIAL");

    public static final Checkpoint COMPLETED_CHECKPOINT_TYPE = new Checkpoint("CHECKPOINT", "COMPLETED");

}
