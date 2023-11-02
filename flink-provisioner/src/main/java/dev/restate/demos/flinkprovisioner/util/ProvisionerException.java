package dev.restate.demos.flinkprovisioner.util;

public class ProvisionerException extends RuntimeException {

    public ProvisionerException(String message) {
        super(message);
    }

    public ProvisionerException(String message, Throwable cause) {
        super(message, cause);
    }
}
