package org.iceforge.runa.semantic.service;

import org.iceforge.runa.semantic.model.DatasetDef;
import org.iceforge.runa.semantic.model.MetricDef;
import org.iceforge.runa.semantic.model.SemanticModel;
import org.iceforge.runa.semantic.web.SemanticQueryRequest;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.stream.Collectors;

@Service
public class SemanticSqlCompiler {

    public CompiledQuery compile(SemanticModel model, SemanticQueryRequest req) {
        Objects.requireNonNull(model);
        Objects.requireNonNull(req);

        if (req.getMetrics().size() != 1) {
            // Keep POC simple: 1 metric at a time. Easy to extend to multiple with same dataset/grain.
            throw new SemanticValidationException("POC supports exactly 1 metric per request for now.");
        }

        String metricName = req.getMetrics().getFirst();
        MetricDef metric = model.getMetrics().get(metricName);
        if (metric == null) {
            throw new SemanticValidationException("Unknown metric: " + metricName);
        }

        DatasetDef ds = model.getDatasets().get(metric.getDataset());
        if (ds == null) {
            throw new SemanticValidationException("Metric '" + metricName + "' references unknown dataset: " + metric.getDataset());
        }

        // Validate group-by
        List<String> groupBy = req.getGroupBy();
        if (metric.getAllowedGroupBy() != null && !metric.getAllowedGroupBy().isEmpty()) {
            for (String g : groupBy) {
                if (!metric.getAllowedGroupBy().contains(g)) {
                    throw new SemanticValidationException("Metric '" + metricName + "' cannot be grouped by '" + g + "'. Allowed: " + metric.getAllowedGroupBy());
                }
            }
        }

        // Ensure required dimensions exist in dataset mapping
        List<String> required = metric.getRequiredDimensions() == null ? List.of() : metric.getRequiredDimensions();
        for (String dim : required) {
            if (ds.getDimensions() == null || !ds.getDimensions().containsKey(dim)) {
                throw new SemanticValidationException("Dataset '" + metric.getDataset() + "' does not provide required dimension '" + dim + "'");
            }
        }

        // Ensure group-by dims exist in dataset mapping
        for (String dim : groupBy) {
            if (ds.getDimensions() == null || !ds.getDimensions().containsKey(dim)) {
                throw new SemanticValidationException("Dataset '" + metric.getDataset() + "' does not provide groupBy dimension '" + dim + "'");
            }
        }

        // Build SELECT list
        List<String> selectCols = new ArrayList<>();
        // Always include cob_date if dataset provides it; it's a very common requirement in risk.
        if (ds.getDimensions() != null && ds.getDimensions().containsKey("cob_date")) {
            selectCols.add(ds.getDimensions().get("cob_date") + " AS cob_date");
        }

        for (String dim : groupBy) {
            String col = ds.getDimensions().get(dim);
            selectCols.add(col + " AS " + dim);
        }

        selectCols.add(metric.getExpression() + " AS " + metricName);

        // WHERE clause: asOf mapped to cob_date by convention (POC)
        List<String> where = new ArrayList<>();
        if (ds.getDimensions() != null && ds.getDimensions().containsKey("cob_date")) {
            // naive literal; in real impl parameterize / validate date format carefully.
            where.add(ds.getDimensions().get("cob_date") + " = DATE '" + escapeSqlLiteral(req.getAsOf()) + "'");
        }

        // Filters
        Map<String, List<String>> filters = req.getFilters() == null ? Map.of() : req.getFilters();
        for (Map.Entry<String, List<String>> e : filters.entrySet()) {
            String dim = e.getKey();
            List<String> vals = e.getValue();
            if (vals == null || vals.isEmpty()) continue;

            String col = ds.getDimensions() == null ? null : ds.getDimensions().get(dim);
            if (col == null) {
                throw new SemanticValidationException("Filter dimension '" + dim + "' not available on dataset '" + metric.getDataset() + "'");
            }

            String in = vals.stream()
                    .map(v -> "'" + escapeSqlLiteral(v) + "'")
                    .collect(Collectors.joining(","));
            where.add(col + " IN (" + in + ")");
        }

        // GROUP BY (only the physical columns, not aliases)
        List<String> groupByCols = new ArrayList<>();
        if (ds.getDimensions() != null && ds.getDimensions().containsKey("cob_date")) {
            groupByCols.add(ds.getDimensions().get("cob_date"));
        }
        for (String dim : groupBy) {
            groupByCols.add(ds.getDimensions().get(dim));
        }

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(String.join(", ", selectCols)).append("\n")
           .append("FROM ").append(ds.getTable()).append("\n");

        if (!where.isEmpty()) {
            sql.append("WHERE ").append(String.join("\n  AND ", where)).append("\n");
        }

        if (!groupByCols.isEmpty()) {
            sql.append("GROUP BY ").append(String.join(", ", groupByCols)).append("\n");
        }

        return new CompiledQuery(metricName, metric.getDataset(), sql.toString());
    }

    private static String escapeSqlLiteral(String s) {
        return s.replace("'", "''");
    }

    public record CompiledQuery(String metric, String dataset, String sql) {}

    public static class SemanticValidationException extends RuntimeException {
        public SemanticValidationException(String message) {
            super(message);
        }
    }
}
