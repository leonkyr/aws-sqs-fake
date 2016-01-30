package com.example.exceptions;

public class QueueDoesNotExistException extends RuntimeException {
    public QueueDoesNotExistException(String message) {
        super(message);
    }
}
