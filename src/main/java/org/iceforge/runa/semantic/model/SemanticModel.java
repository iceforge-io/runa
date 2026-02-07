package org.iceforge.runa.semantic.model;

import java.util.Map;

public class SemanticModel {
    private Map<String, DatasetDef> datasets;
    private Map<String, MetricDef> metrics;
    private Map<String, DimensionDef> dimensions;

    public Map<String, DatasetDef> getDatasets() {
        return datasets;
    }

    public void setDatasets(Map<String, DatasetDef> datasets) {
        this.datasets = datasets;
    }

    public Map<String, MetricDef> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, MetricDef> metrics) {
        this.metrics = metrics;
    }

    public Map<String, DimensionDef> getDimensions() {
        return dimensions;
    }

    public void setDimensions(Map<String, DimensionDef> dimensions) {
        this.dimensions = dimensions;
    }
}
