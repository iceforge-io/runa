# Databricks notebook source
# ============================================================
# MXL/MXR-ish Market Risk Mock Data Generator (Serverless Safe)
# Writes Delta files to a configurable base path (no PERSIST TABLE)
# ============================================================

# COMMAND ----------
# --- Widgets / Config ---
dbutils.widgets.text(
  "base_path",
  "/Volumes/main/default/mxr_data/mxr_mock",
  "Base path (Delta folders)"
)
dbutils.widgets.text("target_schema", "workspace.sandbox", "Target schema for managed tables")
target_schema = dbutils.widgets.get("target_schema")

dbutils.widgets.text("start_date", "2025-01-01", "Start date (YYYY-MM-DD)")
dbutils.widgets.text("end_date", "2025-03-31", "End date (YYYY-MM-DD)")
dbutils.widgets.text("seed", "42", "Random seed")
dbutils.widgets.text("scale", "1.0", "Scale (float): 1.0 baseline, >1 more data")

base_path = dbutils.widgets.get("base_path").rstrip("/")
start_date = dbutils.widgets.get("start_date")
end_date = dbutils.widgets.get("end_date")
seed = int(dbutils.widgets.get("seed"))
scale = float(dbutils.widgets.get("scale"))

print("base_path =", base_path)
print("date range =", start_date, "to", end_date)
print("seed =", seed, "scale =", scale)

# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T
from datetime import datetime, timedelta, date
import random
import math

random.seed(seed)

# COMMAND ----------
# --- Utilities ---
base_path = dbutils.widgets.get("base_path").rstrip("/")

def normalize_base_path(p: str) -> str:
    p = (p or "").strip().rstrip("/")
    # If user passed legacy local paths like /tmp/... or /dbfs/...
    if p.startswith("/tmp/") or p == "/tmp" or p.startswith("/dbfs/"):
        raise ValueError(
            f"base_path '{p}' is not allowed on Serverless when Public DBFS root is disabled. "
            "Use a Unity Catalog Volume: /Volumes/<catalog>/<schema>/<volume>/... "
            "or (if enabled) dbfs:/FileStore/..."
        )
    # If user passed dbfs:/tmp/... which often maps to public DBFS root, block it
    if p.startswith("dbfs:/tmp") or p.startswith("dbfs:/"):
        # FileStore might be permitted; keep it if so
        if not p.startswith("dbfs:/FileStore/"):
            raise ValueError(
                f"base_path '{p}' may be blocked on Serverless (Public DBFS root disabled). "
                "Prefer /Volumes/... or dbfs:/FileStore/..."
            )
    # Volumes are best
    if p.startswith("/Volumes/"):
        return p
    # Allow FileStore if user explicitly chose it
    if p.startswith("dbfs:/FileStore/"):
        return p
    # Otherwise, force the user to choose an allowed location (don’t silently fall back)
    raise ValueError(
        f"base_path '{p}' not recognized as an allowed Serverless path. "
        "Use /Volumes/<catalog>/<schema>/<volume>/... or dbfs:/FileStore/..."
    )

base_path = normalize_base_path(base_path)
print("Using base_path:", base_path)

def parse_ymd(s: str) -> date:
    return datetime.strptime(s, "%Y-%m-%d").date()

def cob_dates_weekdays_ex_holidays(start_ymd: str, end_ymd: str):
    """
    Generate COB dates:
      - Monday..Friday only
      - Exclude Jan 1 and Dec 25 (for each year in range)
    """
    s = parse_ymd(start_ymd)
    e = parse_ymd(end_ymd)
    out = []
    d = s
    while d <= e:
        if d.weekday() < 5:  # Mon-Fri
            if not ((d.month == 1 and d.day == 1) or (d.month == 12 and d.day == 25)):
                out.append(d.isoformat())
        d += timedelta(days=1)
    return out

def write_delta(df, name: str, partition_cols=None, mode="overwrite"):
    full_name = f"{target_schema}.{name}"
    w = df.write.format("delta").mode(mode)
    if partition_cols:
        w = w.partitionBy(*partition_cols)
    w.saveAsTable(full_name)   # <-- managed table, no DBFS root involved
    print(f"Wrote table {full_name} (rows={df.count()})")
    return full_name


def read_delta_as_temp_view(name: str):
    full_name = f"{target_schema}.{name}"
    spark.table(full_name).createOrReplaceTempView(name)
    print(f"Temp view created: {name} (from {full_name})")


# COMMAND ----------
# --- Generate COB dates ---
cob_dates = cob_dates_weekdays_ex_holidays(start_date, end_date)
print("COB dates:", len(cob_dates), "first=", cob_dates[0] if cob_dates else None, "last=", cob_dates[-1] if cob_dates else None)

cob_df = spark.createDataFrame([(d,) for d in cob_dates], ["cob_date"]).withColumn("cob_date", F.to_date("cob_date"))
write_delta(cob_df, "dim_cob_date", partition_cols=None)

# COMMAND ----------
# ============================================================
# 1) 10-level Org Hierarchy (Rates organized by Region/Product/...)
# ============================================================

# We build a 10-level hierarchy with readable columns:
#   org_l1 .. org_l10 + org_node_id + parent_node_id + leaf flags
#
# Rates desks structured:
#   L1: "Rates"
#   L2: Region (NA, EMEA, APAC, LATAM)
#   L3: Product (IRS, Swaps, Options, Govies, Inflation, XCCY)
#   L4..L10: additional deep hierarchy to mimic corporate structures (desk->subdesk->strategy->book cluster etc.)

# COMMAND ----------
regions = ["NA", "EMEA", "APAC", "LATAM"]
products = ["IRS", "SWAPS", "OPTIONS", "GOVIES", "INFLATION", "XCCY"]

# Control how many nodes we generate at each deeper level.
# Keep it serverless-friendly but still "warehouse-painful".
scale_factor = max(0.5, scale)

# deeper level multiplicities
l4_n = int(3 * scale_factor)   # desk groups
l5_n = int(3 * scale_factor)   # desks
l6_n = int(4 * scale_factor)   # subdesks
l7_n = int(3 * scale_factor)   # strategies
l8_n = int(3 * scale_factor)   # book clusters
l9_n = int(4 * scale_factor)   # books
l10_n = int(2 * scale_factor)  # portfolios under books

def mk_code(prefix, i, width=2):
    return f"{prefix}{i:0{width}d}"

nodes = []
node_id = 1
root_id = node_id
nodes.append({
    "org_node_id": node_id,
    "parent_node_id": None,
    "org_l1": "Rates",
    "org_l2": None, "org_l3": None, "org_l4": None, "org_l5": None,
    "org_l6": None, "org_l7": None, "org_l8": None, "org_l9": None, "org_l10": None,
    "is_leaf": False,
    "depth": 1
})
node_id += 1

for r in regions:
    rid = node_id
    nodes.append({
        "org_node_id": rid, "parent_node_id": root_id,
        "org_l1": "Rates", "org_l2": r, "org_l3": None, "org_l4": None, "org_l5": None,
        "org_l6": None, "org_l7": None, "org_l8": None, "org_l9": None, "org_l10": None,
        "is_leaf": False, "depth": 2
    })
    node_id += 1

    for p in products:
        pid = node_id
        nodes.append({
            "org_node_id": pid, "parent_node_id": rid,
            "org_l1": "Rates", "org_l2": r, "org_l3": p,
            "org_l4": None, "org_l5": None, "org_l6": None, "org_l7": None,
            "org_l8": None, "org_l9": None, "org_l10": None,
            "is_leaf": False, "depth": 3
        })
        node_id += 1

        for i4 in range(1, l4_n+1):
            l4 = mk_code("DG", i4)  # Desk Group
            l4id = node_id
            nodes.append({
                "org_node_id": l4id, "parent_node_id": pid,
                "org_l1": "Rates", "org_l2": r, "org_l3": p, "org_l4": l4,
                "org_l5": None, "org_l6": None, "org_l7": None, "org_l8": None, "org_l9": None, "org_l10": None,
                "is_leaf": False, "depth": 4
            })
            node_id += 1

            for i5 in range(1, l5_n+1):
                l5 = mk_code("D", i5)  # Desk
                l5id = node_id
                nodes.append({
                    "org_node_id": l5id, "parent_node_id": l4id,
                    "org_l1": "Rates", "org_l2": r, "org_l3": p, "org_l4": l4, "org_l5": l5,
                    "org_l6": None, "org_l7": None, "org_l8": None, "org_l9": None, "org_l10": None,
                    "is_leaf": False, "depth": 5
                })
                node_id += 1

                for i6 in range(1, l6_n+1):
                    l6 = mk_code("SD", i6)  # Subdesk
                    l6id = node_id
                    nodes.append({
                        "org_node_id": l6id, "parent_node_id": l5id,
                        "org_l1": "Rates", "org_l2": r, "org_l3": p, "org_l4": l4, "org_l5": l5, "org_l6": l6,
                        "org_l7": None, "org_l8": None, "org_l9": None, "org_l10": None,
                        "is_leaf": False, "depth": 6
                    })
                    node_id += 1

                    for i7 in range(1, l7_n+1):
                        l7 = mk_code("ST", i7)  # Strategy
                        l7id = node_id
                        nodes.append({
                            "org_node_id": l7id, "parent_node_id": l6id,
                            "org_l1": "Rates", "org_l2": r, "org_l3": p, "org_l4": l4, "org_l5": l5, "org_l6": l6, "org_l7": l7,
                            "org_l8": None, "org_l9": None, "org_l10": None,
                            "is_leaf": False, "depth": 7
                        })
                        node_id += 1

                        for i8 in range(1, l8_n+1):
                            l8 = mk_code("BC", i8)  # Book Cluster
                            l8id = node_id
                            nodes.append({
                                "org_node_id": l8id, "parent_node_id": l7id,
                                "org_l1": "Rates", "org_l2": r, "org_l3": p, "org_l4": l4, "org_l5": l5, "org_l6": l6, "org_l7": l7, "org_l8": l8,
                                "org_l9": None, "org_l10": None,
                                "is_leaf": False, "depth": 8
                            })
                            node_id += 1

                            for i9 in range(1, l9_n+1):
                                l9 = mk_code("B", i9, width=3)  # Book
                                l9id = node_id
                                nodes.append({
                                    "org_node_id": l9id, "parent_node_id": l8id,
                                    "org_l1": "Rates", "org_l2": r, "org_l3": p, "org_l4": l4, "org_l5": l5, "org_l6": l6, "org_l7": l7, "org_l8": l8, "org_l9": l9,
                                    "org_l10": None,
                                    "is_leaf": False, "depth": 9
                                })
                                node_id += 1

                                for i10 in range(1, l10_n+1):
                                    l10 = mk_code("PF", i10, width=2)  # Portfolio
                                    leaf_id = node_id
                                    nodes.append({
                                        "org_node_id": leaf_id, "parent_node_id": l9id,
                                        "org_l1": "Rates", "org_l2": r, "org_l3": p, "org_l4": l4, "org_l5": l5, "org_l6": l6, "org_l7": l7, "org_l8": l8, "org_l9": l9, "org_l10": l10,
                                        "is_leaf": True, "depth": 10
                                    })
                                    node_id += 1

org_schema = T.StructType([
    T.StructField("org_node_id", T.IntegerType(), False),
    T.StructField("parent_node_id", T.IntegerType(), True),
    *[T.StructField(f"org_l{i}", T.StringType(), True) for i in range(1, 11)],
    T.StructField("is_leaf", T.BooleanType(), False),
    T.StructField("depth", T.IntegerType(), False),
])

org_df = spark.createDataFrame(nodes, schema=org_schema)

# Build a canonical "org_path" and a few keys for joins/drilldowns
org_df = (org_df
          .withColumn("org_path", F.concat_ws("/",
                                              F.col("org_l1"), F.col("org_l2"), F.col("org_l3"), F.col("org_l4"), F.col("org_l5"),
                                              F.col("org_l6"), F.col("org_l7"), F.col("org_l8"), F.col("org_l9"), F.col("org_l10")))
          .withColumn("org_leaf_id", F.when(F.col("is_leaf"), F.col("org_node_id")).otherwise(F.lit(None).cast("int")))
          .withColumn("region", F.col("org_l2"))
          .withColumn("product", F.col("org_l3"))
         )

write_delta(org_df, "dim_org_hierarchy", partition_cols=None)
read_delta_as_temp_view("dim_org_hierarchy")

# COMMAND ----------
# ============================================================
# 2) Risk Factor Hierarchy + Curves/Tenors
# ============================================================

# We'll create RFs like:
#   Rates / IR / {CCY} / {INDEX} / {Curve} / {Tenor}
# plus some "spread/vol" style factors to increase variety.

ccys = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD"]
indices = {
    "USD": ["SOFR", "LIBOR3M"],
    "EUR": ["ESTR", "EURIBOR3M"],
    "GBP": ["SONIA"],
    "JPY": ["TONA"],
    "CAD": ["CORRA"],
    "AUD": ["AONIA"],
}
tenors = ["1D","1W","1M","3M","6M","1Y","2Y","5Y","10Y","30Y"]

rf_rows = []
curve_rows = []
rf_id = 1
curve_id = 1

for ccy in ccys:
    for idx in indices.get(ccy, ["OIS"]):
        curve_name = f"{ccy}_{idx}_ZERO"
        curve_rows.append((curve_id, "IR_ZERO", ccy, idx, curve_name))
        this_curve_id = curve_id
        curve_id += 1

        for tnr in tenors:
            rf_rows.append((
                rf_id,
                "Rates", "IR", ccy, idx,
                curve_name,
                tnr,
                "ZERO_RATE",
                this_curve_id
            ))
            rf_id += 1

# Add some spread + vol factors (smaller set, still helpful)
spreads = ["ASW", "OAS", "BASIS"]
vols = ["IR_VOL", "SWAPTION_VOL"]
for ccy in ccys:
    for sp in spreads:
        rf_rows.append((rf_id, "Rates", "SPREAD", ccy, None, f"{ccy}_{sp}", "5Y", sp, None))
        rf_id += 1
    for v in vols:
        rf_rows.append((rf_id, "Rates", "VOL", ccy, None, f"{ccy}_{v}", "1Y", v, None))
        rf_id += 1

rf_schema = T.StructType([
    T.StructField("risk_factor_id", T.IntegerType(), False),
    T.StructField("rf_l1_asset_class", T.StringType(), False),
    T.StructField("rf_l2_type", T.StringType(), False),
    T.StructField("rf_l3_ccy", T.StringType(), True),
    T.StructField("rf_l4_index", T.StringType(), True),
    T.StructField("rf_l5_curve", T.StringType(), False),
    T.StructField("rf_l6_tenor", T.StringType(), True),
    T.StructField("rf_name", T.StringType(), False),
    T.StructField("curve_id", T.IntegerType(), True),
])

curve_schema = T.StructType([
    T.StructField("curve_id", T.IntegerType(), False),
    T.StructField("curve_type", T.StringType(), False),
    T.StructField("ccy", T.StringType(), False),
    T.StructField("index_name", T.StringType(), True),
    T.StructField("curve_name", T.StringType(), False),
])

rf_df = spark.createDataFrame(rf_rows, schema=rf_schema)
curve_df = spark.createDataFrame(curve_rows, schema=curve_schema)

write_delta(curve_df, "dim_curve", partition_cols=None)
write_delta(rf_df, "dim_risk_factor", partition_cols=["rf_l3_ccy"])  # mild partition, not too many
read_delta_as_temp_view("dim_risk_factor")
read_delta_as_temp_view("dim_curve")

# COMMAND ----------
# ============================================================
# 3) Books/Portfolios mapping (from org leaf nodes)
# ============================================================

# Use leaf org nodes as "portfolio granularity" and assign:
#   - portfolio_id = org_leaf_id
#   - book_id is derived from org_l9 + higher path to keep realistic uniqueness

leaf_org = org_df.filter("is_leaf = true").select(
    "org_leaf_id", "org_path", "region", "product",
    *[f"org_l{i}" for i in range(1,11)]
)

# Create deterministic IDs
port_df = (leaf_org
           .withColumn("portfolio_id", F.col("org_leaf_id").cast("long"))
           .withColumn("book_code", F.concat_ws("_", F.col("org_l2"), F.col("org_l3"), F.col("org_l4"), F.col("org_l5"), F.col("org_l9")))
           .withColumn("book_id", F.abs(F.xxhash64("book_code")).cast("long"))
           .withColumn("desk_code", F.concat_ws("_", F.col("org_l2"), F.col("org_l3"), F.col("org_l4"), F.col("org_l5"), F.col("org_l6")))
           .withColumn("desk_id", F.abs(F.xxhash64("desk_code")).cast("long"))
           .select(
               "portfolio_id","book_id","desk_id",
               "region","product","org_path",
               *[F.col(f"org_l{i}").alias(f"org_l{i}") for i in range(1,11)]
           )
)

write_delta(port_df, "dim_portfolio", partition_cols=["region"])
read_delta_as_temp_view("dim_portfolio")

# COMMAND ----------
# ============================================================
# 4) Positions (COB keyed) + Instruments
# ============================================================

# We'll generate positions per COB per portfolio with skewed distributions:
# - more positions in NA/USD IRS vs smaller elsewhere
# - include some massive portfolios to create group-by skew pain

# COMMAND ----------
# Position scale controls
# baseline sizes (scaled)
base_positions_per_cob = int(12000 * scale_factor)   # total positions per COB across all portfolios
big_portfolio_frac = 0.02                            # 2% portfolios are "huge"
big_portfolio_multiplier = 15                        # huge portfolios get ~15x more positions weighting

print("positions/cob baseline:", base_positions_per_cob)

# Build portfolio weights for skew
port_w = (port_df
          .withColumn("is_big", (F.rand(seed) < F.lit(big_portfolio_frac)))
          .withColumn("w_region", F.when(F.col("region")=="NA", 3.0).when(F.col("region")=="EMEA", 2.0).otherwise(1.0))
          .withColumn("w_product", F.when(F.col("product")=="IRS", 3.0).when(F.col("product")=="SWAPS", 2.0).otherwise(1.0))
          .withColumn("weight", F.col("w_region")*F.col("w_product")*F.when(F.col("is_big"), F.lit(big_portfolio_multiplier)).otherwise(F.lit(1.0)))
          .select("portfolio_id","book_id","desk_id","region","product","org_path","weight",
                  *[f"org_l{i}" for i in range(1,11)])
)

# We'll sample positions by creating a "positions_count" per portfolio per COB
# (not exact to base_positions_per_cob, but close and stable)
w_sum = port_w.agg(F.sum("weight").alias("wsum")).collect()[0]["wsum"]

pos_counts = (port_w
              .withColumn("positions_per_cob",
                          F.greatest(F.lit(1),
                                     (F.col("weight")/F.lit(w_sum) * F.lit(base_positions_per_cob)).cast("int")))
             )

# Create positions across COB dates via explode and sequence generation per portfolio
# Use array_repeat + posexplode to create N rows per portfolio per COB
cob_sdf = cob_df.select("cob_date")

pos_plan = (pos_counts
            .crossJoin(cob_sdf)
            .withColumn("n", F.col("positions_per_cob"))
            .withColumn("rep", F.expr("sequence(1, n)"))
            .select(
                "cob_date","portfolio_id","book_id","desk_id","region","product","org_path",
                *[f"org_l{i}" for i in range(1,11)],
                F.posexplode("rep").alias("pos_idx","_")
            )
)

# Create instrument attributes
instr_types = ["SWAP", "SWAPTION", "BOND", "FUTURE"]
ccy_choices = ["USD","EUR","GBP","JPY","CAD","AUD"]

positions_df = (pos_plan
    .withColumn("position_id", F.abs(F.xxhash64("cob_date","portfolio_id","pos_idx")).cast("long"))
    .withColumn("trade_id", F.abs(F.xxhash64("portfolio_id","pos_idx")).cast("long"))
    .withColumn("instrument_type", F.element_at(F.array([F.lit(x) for x in instr_types]), (F.pmod(F.xxhash64("position_id"), F.lit(len(instr_types)))+1).cast("int")))
    .withColumn("ccy", F.element_at(F.array([F.lit(x) for x in ccy_choices]), (F.pmod(F.xxhash64("position_id","portfolio_id"), F.lit(len(ccy_choices)))+1).cast("int")))
    .withColumn("notional", (F.rand(seed)*F.lit(5e7) + F.lit(1e6)) * F.when(F.col("instrument_type")=="BOND", 0.2).otherwise(1.0))
    .withColumn("maturity_bucket", F.element_at(F.array([F.lit(x) for x in ["<1Y","1-2Y","2-5Y","5-10Y",">10Y"]]),
                                               (F.pmod(F.xxhash64("position_id","cob_date"), F.lit(5))+1).cast("int")))
    .withColumn("is_active", F.lit(True))
    .select(
        "cob_date","position_id","trade_id",
        "portfolio_id","book_id","desk_id",
        "region","product","org_path",
        *[F.col(f"org_l{i}").alias(f"org_l{i}") for i in range(1,11)],
        "instrument_type","ccy","maturity_bucket","notional","is_active"
    )
)

write_delta(positions_df, "fact_position", partition_cols=["cob_date","region"])
read_delta_as_temp_view("fact_position")

# COMMAND ----------
# ============================================================
# 5) Sensitivities / Greeks (COB keyed) - main "pain point" table
# ============================================================

# We assign each position a handful of risk factors:
# - IR positions: many tenors on a curve (bucketed)
# - options: add vega/gamma
# We'll also create *skew* and *high-cardinality* columns to stress drilldowns.

# COMMAND ----------
greeks = ["DV01", "PV01", "GAMMA", "VEGA"]
bump_types = ["PAR", "ZERO", "VOL", "SPREAD"]

rf_small = rf_df.select("risk_factor_id","rf_l2_type","rf_l3_ccy","rf_l5_curve","rf_l6_tenor","rf_name")

# Choose a subset size per position
min_rf = 4
max_rf = 14

# Use deterministic pseudo-random counts per position
sens_base = (positions_df
             .withColumn("rf_count", (F.pmod(F.xxhash64("position_id"), F.lit(max_rf-min_rf+1)) + F.lit(min_rf)).cast("int"))
             .withColumn("rf_seq", F.expr("sequence(1, rf_count)"))
             .select("*", F.posexplode("rf_seq").alias("rf_idx","_"))
)

# Map each rf_idx to a risk_factor_id deterministically; prefer matching currency sometimes
# We'll do:
#   rf_id = 1 + (hash(position_id, rf_idx) mod rf_total)
rf_total = rf_df.count()

sens_df = (sens_base
    .withColumn("risk_factor_id",
                (F.pmod(F.abs(F.xxhash64("position_id","rf_idx")), F.lit(rf_total)) + F.lit(1)).cast("int"))
    .join(rf_small, on="risk_factor_id", how="left")
    .withColumn("greek",
                F.when(F.col("instrument_type")=="SWAPTION",
                       F.element_at(F.array([F.lit("DV01"),F.lit("GAMMA"),F.lit("VEGA")]),
                                    (F.pmod(F.xxhash64("position_id","rf_idx","cob_date"), F.lit(3))+1).cast("int")))
                 .otherwise(
                       F.element_at(F.array([F.lit("DV01"),F.lit("PV01")]),
                                    (F.pmod(F.xxhash64("position_id","rf_idx"), F.lit(2))+1).cast("int")))
                )
    .withColumn("bump_type",
                F.when(F.col("rf_l2_type")=="VOL", F.lit("VOL"))
                 .when(F.col("rf_l2_type")=="SPREAD", F.lit("SPREAD"))
                 .otherwise(F.lit("ZERO")))
    # Sensitivity magnitude (scaled by notional, greek type, and some noise)
    .withColumn("sensitivity",
                (F.col("notional")/F.lit(1e6)) *
                (F.when(F.col("greek")=="DV01", F.lit(0.8))
                 .when(F.col("greek")=="PV01", F.lit(0.6))
                 .when(F.col("greek")=="GAMMA", F.lit(0.05))
                 .otherwise(F.lit(0.03))) *
                (F.rand(seed)*F.lit(2.0) - F.lit(1.0))
               )
    # High-cardinality-ish tag to mimic "risk run id" / "calc batch id"
    .withColumn("calc_batch_id", F.concat(F.lit("B"), F.date_format("cob_date","yyyyMMdd"), F.lit("_"), F.substring(F.sha2(F.concat_ws(":",F.col("book_id"),F.col("desk_id")), 256), 1, 8)))
    .select(
        "cob_date",
        "position_id","trade_id",
        "portfolio_id","book_id","desk_id",
        "region","product","org_path",
        *[F.col(f"org_l{i}").alias(f"org_l{i}") for i in range(1,11)],
        "instrument_type","ccy","maturity_bucket","notional",
        "risk_factor_id","rf_l2_type","rf_l3_ccy","rf_l5_curve","rf_l6_tenor","rf_name",
        "greek","bump_type","sensitivity",
        "calc_batch_id"
    )
)

write_delta(sens_df, "fact_sensitivity", partition_cols=["cob_date","region"])
read_delta_as_temp_view("fact_sensitivity")

# COMMAND ----------
# ============================================================
# 6) Tweak #2: Denormalized "Wide" Sensitivity Fact (join-avoidance pattern)
# ============================================================

# This table intentionally repeats org + rf hierarchy fields and pre-buckets
# to simulate the wide/denormalized drilldown dataset common in warehouses.

sens_wide = (sens_df
             .withColumn("rf_bucket",
                         F.when(F.col("rf_l2_type")=="IR", F.concat_ws("_", F.col("rf_l3_ccy"), F.col("rf_l6_tenor")))
                          .when(F.col("rf_l2_type")=="VOL", F.concat_ws("_", F.col("rf_l3_ccy"), F.lit("VOL")))
                          .otherwise(F.concat_ws("_", F.col("rf_l3_ccy"), F.lit("SPREAD"))))
             .withColumn("tenor_years",
                         F.when(F.col("rf_l6_tenor").endswith("Y"), F.regexp_extract("rf_l6_tenor", r"(\d+)", 1).cast("int"))
                          .when(F.col("rf_l6_tenor").endswith("M"), (F.regexp_extract("rf_l6_tenor", r"(\d+)", 1).cast("int")/F.lit(12.0)))
                          .otherwise(F.lit(None).cast("double")))
             .select(
                 "cob_date","calc_batch_id",
                 "desk_id","book_id","portfolio_id","position_id",
                 "region","product",
                 *[f"org_l{i}" for i in range(1,11)],
                 "instrument_type","ccy","maturity_bucket","notional",
                 "risk_factor_id","rf_l2_type","rf_l3_ccy","rf_l5_curve","rf_l6_tenor","rf_bucket","tenor_years",
                 "greek","bump_type","sensitivity"
             ))

write_delta(sens_wide, "fact_sens_wide", partition_cols=["cob_date","region"])
read_delta_as_temp_view("fact_sens_wide")

# COMMAND ----------
# ============================================================
# 7) Scenarios + Scenario Shocks on Risk Factors
# ============================================================

# We create a scenario set per COB with:
# - BASE
# - parallel shifts by region/ccy
# - curve steepeners/flatteners
# - vol up/down
# - spread widen/tighten

# COMMAND ----------
scenario_types = [
    ("BASE", 0.0),
    ("IR_PARALLEL_UP", 1.0),
    ("IR_PARALLEL_DN", -1.0),
    ("IR_STEEPENER", 1.0),
    ("IR_FLATTENER", -1.0),
    ("VOL_UP", 1.0),
    ("VOL_DN", -1.0),
    ("SPREAD_WIDEN", 1.0),
    ("SPREAD_TIGHTEN", -1.0),
]

# Scenario dimension (stable)
scen_dim = spark.createDataFrame(
    [(i+1, scenario_types[i][0], float(scenario_types[i][1])) for i in range(len(scenario_types))],
    ["scenario_id","scenario_name","direction"]
)

write_delta(scen_dim, "dim_scenario", partition_cols=None)
read_delta_as_temp_view("dim_scenario")

# Scenario shocks: per COB + scenario + risk_factor_id -> shock value
# Keep BASE shocks = 0
rf_for_shocks = rf_df.select("risk_factor_id","rf_l2_type","rf_l3_ccy","rf_l6_tenor")

scen_cob = cob_df.crossJoin(scen_dim)

# Shock sizing heuristics
shocks = (scen_cob
          .crossJoin(rf_for_shocks)
          .withColumn("shock",
                      F.when(F.col("scenario_name")=="BASE", F.lit(0.0))
                       .when(F.col("scenario_name").startswith("IR_"),
                             F.when(F.col("rf_l2_type")=="IR",
                                    # Tenor-shaped shocks for steepener/flatteners
                                    F.when(F.col("scenario_name")=="IR_PARALLEL_UP", F.lit(1.0))
                                     .when(F.col("scenario_name")=="IR_PARALLEL_DN", F.lit(-1.0))
                                     .otherwise(
                                         F.when(F.col("rf_l6_tenor").endswith("Y"),
                                                (F.regexp_extract("rf_l6_tenor", r"(\d+)", 1).cast("double")/F.lit(10.0)))
                                          .otherwise(F.lit(0.1))
                                     ) * F.col("direction") * F.lit(1e-4)
                                   ).otherwise(F.lit(0.0)))
                       .when(F.col("scenario_name").startswith("VOL_"),
                             F.when(F.col("rf_l2_type")=="VOL", F.col("direction")*F.lit(0.10)).otherwise(F.lit(0.0)))
                       .when(F.col("scenario_name").startswith("SPREAD_"),
                             F.when(F.col("rf_l2_type")=="SPREAD", F.col("direction")*F.lit(5e-4)).otherwise(F.lit(0.0)))
                       .otherwise(F.lit(0.0))
                     )
          .select("cob_date","scenario_id","scenario_name","risk_factor_id","shock")
)

write_delta(shocks, "fact_scenario_shock", partition_cols=["cob_date"])
read_delta_as_temp_view("fact_scenario_shock")

# COMMAND ----------
# ============================================================
# 8) PnL from Sensitivities x Shocks (COB, scenario, position)
# ============================================================

# We'll compute:
#   pnl = - sensitivity * shock * scaling_by_greek
# and aggregate also to desk/book/portfolio rollups.

# COMMAND ----------
# Scale by greek to keep numbers in plausible ranges
pnl_scale = (F.when(F.col("greek")=="DV01", F.lit(1.0))
             .when(F.col("greek")=="PV01", F.lit(1.0))
             .when(F.col("greek")=="GAMMA", F.lit(0.3))
             .otherwise(F.lit(0.2)))

pnl_fact = (sens_df.alias("s")
            .join(shocks.alias("k"),
                  on=[F.col("s.cob_date")==F.col("k.cob_date"),
                      F.col("s.risk_factor_id")==F.col("k.risk_factor_id")],
                  how="inner")
            .withColumn("pnl", -F.col("s.sensitivity") * F.col("k.shock") * pnl_scale)
            .select(
                F.col("s.cob_date").alias("cob_date"),
                F.col("k.scenario_id").alias("scenario_id"),
                F.col("k.scenario_name").alias("scenario_name"),
                "s.position_id","s.portfolio_id","s.book_id","s.desk_id",
                "s.region","s.product",
                *[F.col(f"s.org_l{i}").alias(f"org_l{i}") for i in range(1,11)],
                "s.instrument_type","s.ccy",
                "s.risk_factor_id","s.greeks" if False else F.col("s.greek").alias("greek"),
                "pnl"
            )
)

write_delta(pnl_fact, "fact_pnl_scenario_position", partition_cols=["cob_date","scenario_id"])
read_delta_as_temp_view("fact_pnl_scenario_position")

# COMMAND ----------
# ============================================================
# 9) VaR (toy) from scenario PnL distribution
# ============================================================

# We'll compute a simple historical-sim style VaR by:
# - aggregating PnL per COB per desk/book/portfolio by scenario
# - taking percentile of loss (e.g., 99% VaR => 1st percentile of PnL, then negate)

# COMMAND ----------
var_level = 0.99  # 99% VaR

pnl_roll = (pnl_fact
            .groupBy("cob_date","scenario_id","scenario_name","desk_id","book_id","portfolio_id","region","product",
                     *[f"org_l{i}" for i in range(1,11)])
            .agg(F.sum("pnl").alias("pnl"))
)

# percentile_approx expects a probability of 1-var_level for left tail
tail_prob = float(1.0 - var_level)

var_fact = (pnl_roll
            .groupBy("cob_date","desk_id","book_id","portfolio_id","region","product",
                     *[f"org_l{i}" for i in range(1,11)])
            .agg(
                F.expr(f"percentile_approx(pnl, {tail_prob}, 10000)").alias("pnl_tail"),
                F.countDistinct("scenario_id").alias("scenario_count"),
                F.sum("pnl").alias("pnl_sum_all_scen")
            )
            .withColumn("var_{:0.0f}".format(var_level*100), -F.col("pnl_tail"))
            .drop("pnl_tail")
)

write_delta(var_fact, "fact_var", partition_cols=["cob_date","region"])
read_delta_as_temp_view("fact_var")

# COMMAND ----------
# ============================================================
# 10) Tweak #1: Pre-aggregated Rollups for Drilldowns (warehouse pain + accelerators)
# ============================================================

# These are the "materialized view style" tables that users often end up wanting.
# We'll aggregate sensitivities by:
#  - COB + org levels (l2..l10) + greek + rf_type/ccy/tenor
#  - COB + risk-factor hierarchy
#
# This directly targets performance pain points: wide group-bys, deep hierarchies, high cardinality.

# COMMAND ----------
# Rollup by COB + org levels + greek + rf_type + ccy + tenor
agg_sens_org = (sens_df
                .groupBy("cob_date","region","product",
                         *[f"org_l{i}" for i in range(1,11)],
                         "greek","rf_l2_type","rf_l3_ccy","rf_l6_tenor")
                .agg(
                    F.sum("sensitivity").alias("sens_sum"),
                    F.countDistinct("position_id").alias("pos_count"),
                    F.countDistinct("risk_factor_id").alias("rf_count")
                ))

write_delta(agg_sens_org, "agg_sens_by_org", partition_cols=["cob_date","region"])
read_delta_as_temp_view("agg_sens_by_org")

# Rollup by COB + risk-factor hierarchy only (cross-desk aggregation)
agg_sens_rf = (sens_df
               .groupBy("cob_date","rf_l2_type","rf_l3_ccy","rf_l5_curve","rf_l6_tenor","greek")
               .agg(
                   F.sum("sensitivity").alias("sens_sum"),
                   F.countDistinct("position_id").alias("pos_count"),
                   F.countDistinct("desk_id").alias("desk_count")
               ))

write_delta(agg_sens_rf, "agg_sens_by_rf", partition_cols=["cob_date","rf_l3_ccy"])
read_delta_as_temp_view("agg_sens_by_rf")

# COMMAND ----------
# ============================================================
# 11) Convenience: Create temp views for all tables (serverless-safe)
# ============================================================

for v in [
    "dim_cob_date",
    "dim_org_hierarchy",
    "dim_portfolio",
    "dim_curve",
    "dim_risk_factor",
    "dim_scenario",
    "fact_position",
    "fact_sensitivity",
    "fact_sens_wide",
    "fact_scenario_shock",
    "fact_pnl_scenario_position",
    "fact_var",
    "agg_sens_by_org",
    "agg_sens_by_rf",
]:
    read_delta_as_temp_view(v)

# COMMAND ----------
# ============================================================
# 12) Example Drilldown Queries (typical warehouse pain points)
# ============================================================

# A) Deep hierarchy rollup for a COB (many group-by keys)
example_cob = cob_dates[-1] if cob_dates else None
print("Example COB:", example_cob)

if example_cob:
    spark.sql(f"""
    SELECT cob_date, org_l2, org_l3, org_l4, org_l5, org_l6, org_l7, org_l8, org_l9, org_l10,
           greek, rf_l2_type, rf_l3_ccy, rf_l6_tenor,
           SUM(sensitivity) AS sens_sum,
           COUNT(DISTINCT position_id) AS pos_cnt
    FROM fact_sensitivity
    WHERE cob_date = DATE('{example_cob}')
      AND org_l2 = 'NA'
      AND org_l3 = 'IRS'
    GROUP BY cob_date, org_l2, org_l3, org_l4, org_l5, org_l6, org_l7, org_l8, org_l9, org_l10,
             greek, rf_l2_type, rf_l3_ccy, rf_l6_tenor
    ORDER BY abs(sens_sum) DESC
    LIMIT 50
    """).display()

# B) Same question, but hitting the pre-aggregated rollup (faster pattern)
if example_cob:
    spark.sql(f"""
    SELECT *
    FROM agg_sens_by_org
    WHERE cob_date = DATE('{example_cob}')
      AND org_l2 = 'NA'
      AND org_l3 = 'IRS'
    ORDER BY abs(sens_sum) DESC
    LIMIT 50
    """).display()

# C) VaR by region/product
if example_cob:
    spark.sql(f"""
    SELECT cob_date, region, product,
           SUM(var_99) AS var_99_sum
    FROM fact_var
    WHERE cob_date = DATE('{example_cob}')
    GROUP BY cob_date, region, product
    ORDER BY var_99_sum DESC
    """).display()

# COMMAND ----------
# Done
print("Done. Delta tables are under:", f"{base_path}/tables/")
