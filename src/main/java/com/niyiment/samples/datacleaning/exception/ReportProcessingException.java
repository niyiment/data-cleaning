package com.niyiment.samples.datacleaning.exception;

public class ReportProcessingException extends RuntimeException {
    public ReportProcessingException(String message) {
        super(message);
    }

    public ReportProcessingException(String message, Throwable cause) {
        super(message, cause);
    }
}
