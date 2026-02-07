package org.iceforge.runa.semantic.model;

import java.util.List;

public class MetricDef {
    private String description;
    private String dataset;
    private String expression; // e.g. SUM(var_99_10d_usd)
    private String grain;
    private List<String> allowedGroupBy;
    private List<String> requiredDimensions;
    private boolean nonAdditive;

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDataset() {
        return dataset;
    }

    public void setDataset(String dataset) {
        this.dataset = dataset;
    }

    public String getExpression() {
        return expression;
    }

    public void setExpression(String expression) {
        this.expression = expression;
    }

    public String getGrain() {
        return grain;
    }

    public void setGrain(String grain) {
        this.grain = grain;
    }

    public List<String> getAllowedGroupBy() {
        return allowedGroupBy;
    }

    public void setAllowedGroupBy(List<String> allowedGroupBy) {
        this.allowedGroupBy = allowedGroupBy;
    }

    public List<String> getRequiredDimensions() {
        return requiredDimensions;
    }

    public void setRequiredDimensions(List<String> requiredDimensions) {
        this.requiredDimensions = requiredDimensions;
    }

    public boolean isNonAdditive() {
        return nonAdditive;
    }

    public void setNonAdditive(boolean nonAdditive) {
        this.nonAdditive = nonAdditive;
    }
}
