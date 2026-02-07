package org.iceforge.runa.semantic.model;

import java.util.Map;

public class DatasetDef {
    private String table;
    private String grain; // e.g. desk, book, position
    private Map<String, String> dimensions; // semanticDim -> physicalColumn

    public String getTable() {
        return table;
    }

    public void setTable(String table) {
        this.table = table;
    }

    public String getGrain() {
        return grain;
    }

    public void setGrain(String grain) {
        this.grain = grain;
    }

    public Map<String, String> getDimensions() {
        return dimensions;
    }

    public void setDimensions(Map<String, String> dimensions) {
        this.dimensions = dimensions;
    }
}
