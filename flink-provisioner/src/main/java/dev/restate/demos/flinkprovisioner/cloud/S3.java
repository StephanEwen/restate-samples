package dev.restate.demos.flinkprovisioner.cloud;

import dev.restate.demos.flinkprovisioner.util.ProvisionerException;

import java.util.Optional;
import java.util.regex.Pattern;

/**
 * This is a mock class for S3 operations.
 *
 * The code simulates that the operations take time, provide async callbacks, and even propagate
 * their "not yet ready" status in a naive way via exceptions.
 */
public final class S3 {

    private static final Pattern S3_URL_PATTERN = Pattern.compile("s3:\\/\\/([^\\/]+)\\/(.+)");

    public static void ensureBucketExists(String bucket) {
        // simulate some failures
        if (Math.random() < 0.5) {
            throw new ProvisionerException("mystic AWS error");
        }

        // operation takes a short amount of time
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static void delete(String pathPrefix) {
        // simulate some failures
        if (Math.random() < 0.5) {
            throw new ProvisionerException("mystic AWS error");
        }

        // operation takes a while
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }


    public static Optional<String> verifyUriAndExtractBucketName(String path) {
        final var matcher = S3_URL_PATTERN.matcher(path);
        if (matcher.matches()) {
            return Optional.of(matcher.group(1));
        } else {
            return Optional.empty();
        }
    }
}
