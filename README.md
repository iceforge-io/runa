# RUNA Semantic Layer (POC)

RUNA is a market-risk semantic layer service designed to sit *in front of* Skadi.

It:
- loads a risk semantic model (`semantic-model.yml`)
- validates a semantic query (metric + dimensions + grain rules)
- compiles it to SQL
- submits the SQL to **Skadi**
- extends to support multiple internal DSL query apis
- streams **Arrow IPC** back to the caller

> Note: This POC currently supports **one metric per request** to keep the compiler simple.

## Run

### 1) Start Skadi
Run your Skadi server (default base URL in this project is `http://localhost:8080`).

### 2) Start Runa
```bash
mvn spring-boot:run
```

Runa starts on port `8090` by default.

## Configure

Edit `src/main/resources/application.yml`:

- `runa.skadiBaseUrl` – where Skadi is running
- `warehouse.*` – JDBC settings passed through to Skadi (POC shortcut)

If your Skadi deployment uses a different Arrow result endpoint, update:

- `runa.skadiArrowResultPathTemplate`

## Example semantic query

POST a semantic query:

```bash
curl -X POST http://localhost:8090/api/semantic/query \
  -H "Content-Type: application/json" \
  -H "Accept: application/vnd.apache.arrow.stream" \
  -d '{
    "asOf": "2026-02-06",
    "metrics": ["var_99_10d_usd"],
    "groupBy": ["desk"],
    "filters": {"desk": ["FX","IR"]},
    "mode": "interactive"
  }' --output result.arrow
```

## Semantic model format

See `src/main/resources/semantic-model.yml`.

You can add more:
- datasets
- metrics
- allowed group-by dimensions
- required dimensions (e.g. scenario_set, currency, run_id)

## Next steps (recommended)
1. Add multi-metric support when all metrics share the same dataset + grain
2. Add query planning: choose different datasets by mode (interactive vs audit)
3. Add a dimension join-graph (to support multi-dataset metrics)
4. Add entitlements / row-level security hooks
5. Add a catalog endpoint (`GET /api/semantic/catalog`) for UI discovery
