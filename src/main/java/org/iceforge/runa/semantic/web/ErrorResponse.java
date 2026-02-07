package org.iceforge.runa.semantic.web;

import java.time.Instant;

public class ErrorResponse {
    private final Instant timestamp = Instant.now();
    private final String error;
    private final String detail;

    public ErrorResponse(String error, String detail) {
        this.error = error;
        this.detail = detail;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getError() {
        return error;
    }

    public String getDetail() {
        return detail;
    }
}
