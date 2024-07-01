package com.zecops.example.mongo;

public class ZipReadingRuntimeException extends RuntimeException {

    public ZipReadingRuntimeException(String message) {
        super(message);
    }

    public ZipReadingRuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
