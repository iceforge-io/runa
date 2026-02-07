package org.iceforge.runa.semantic.web;

/**
 * Conservative shape: Skadi may return more fields; we only need queryId.
 */
public class SkadiSubmitResponse {
    private String queryId;

    public String getQueryId() {
        return queryId;
    }

    public void setQueryId(String queryId) {
        this.queryId = queryId;
    }
}
