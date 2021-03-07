package com.detica.cyberreveal.storm.bolt.exception;

public class BoltIORuntimeException extends RuntimeException {

    public BoltIORuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
