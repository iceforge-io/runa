package org.iceforge.runa.semantic.web;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;

import java.util.List;
import java.util.Map;

public class SemanticQueryRequest {

    /**
     * COB date (yyyy-MM-dd) or a resolved as-of string for your environment.
     * For a first POC we treat it as a literal date string inserted into SQL as DATE '...'.
     */
    @NotBlank
    private String asOf;

    @NotEmpty
    private List<String> metrics;

    /**
     * Dimensions to group by, e.g. ["desk"] or ["desk","book"].
     */
    @NotEmpty
    private List<String> groupBy;

    /**
     * Dimension filters, e.g. {"desk":["FX","IR"], "book":["BK1"]}.
     */
    private Map<String, List<String>> filters;

    /**
     * interactive | audit (affects future planning; for now just echoed).
     */
    private String mode = "interactive";

    public String getAsOf() {
        return asOf;
    }

    public void setAsOf(String asOf) {
        this.asOf = asOf;
    }

    public List<String> getMetrics() {
        return metrics;
    }

    public void setMetrics(List<String> metrics) {
        this.metrics = metrics;
    }

    public List<String> getGroupBy() {
        return groupBy;
    }

    public void setGroupBy(List<String> groupBy) {
        this.groupBy = groupBy;
    }

    public Map<String, List<String>> getFilters() {
        return filters;
    }

    public void setFilters(Map<String, List<String>> filters) {
        this.filters = filters;
    }

    public String getMode() {
        return mode;
    }

    public void setMode(String mode) {
        this.mode = mode;
    }
}
