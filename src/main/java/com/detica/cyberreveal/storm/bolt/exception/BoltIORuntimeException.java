package com.detica.cyberreveal.storm.bolt.exception;

/**
 * A custom exception, BoltIORuntimeException wraps all unchecked standard Javas exception relating to the IO file
 * for a Bolt class.
 */
public class BoltIORuntimeException extends RuntimeException {

    public BoltIORuntimeException(String message, Throwable cause) {
        super(message, cause);
    }
}
