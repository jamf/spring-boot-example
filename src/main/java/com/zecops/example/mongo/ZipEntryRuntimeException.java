package com.zecops.example.mongo;

public class ZipEntryRuntimeException extends RuntimeException {

    public ZipEntryRuntimeException(String message) {
        super(message);
    }

    public ZipEntryRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
