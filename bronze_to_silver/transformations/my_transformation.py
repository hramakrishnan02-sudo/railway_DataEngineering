from pyspark import pipelines as dp
from pyspark.sql.functions import (
    col,
    expr,
    trim,
    upper,
    initcap,
    when,
    to_timestamp,
    to_date
)


# ============================================================
# BRONZE TABLES
# ============================================================

BRONZE_TRAINS = "motordatabricks.bronze.trains"
BRONZE_SCHEDULES = "motordatabricks.bronze.schedules"
BRONZE_WORKORDERS = "motordatabricks.bronze.maintenance_workorders"


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def clean_string(column_name):
    """
    Trim whitespace and convert blank strings to NULL.
    """
    return when(
        trim(col(column_name)) == "",
        None
    ).otherwise(
        trim(col(column_name))
    )


# ============================================================
# 1. TRAINS CDF + SILVER TRANSFORMATION
# ============================================================

@dp.view(name="trains_cdf")
def trains_cdf():

    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(BRONZE_TRAINS)

        # ----------------------------------------------------
        # Basic string cleaning
        # ----------------------------------------------------
        .withColumn(
            "train_number",
            clean_string("train_number")
        )

        .withColumn(
            "train_name",
            clean_string("train_name")
        )

        .withColumn(
            "train_type",
            clean_string("train_type")
        )

        .withColumn(
            "current_station",
            clean_string("current_station")
        )

        # ----------------------------------------------------
        # Standardization
        # ----------------------------------------------------
        .withColumn(
            "train_number",
            upper(col("train_number"))
        )

        .withColumn(
            "train_type",
            initcap(col("train_type"))
        )

        .withColumn(
            "train_name",
            initcap(col("train_name"))
        )

        .withColumn(
            "current_station",
            initcap(col("current_station"))
        )

        .withColumn(
            "current_status",
            upper(trim(col("current_status")))
        )

        # ----------------------------------------------------
        # Explicit datatype conversion
        # ----------------------------------------------------
        .withColumn(
            "train_id",
            col("train_id").cast("int")
        )

        .withColumn(
            "updated_at",
            to_timestamp(col("updated_at"))
        )

        # ----------------------------------------------------
        # Data quality
        # Don't allow CDC records without the primary key
        # ----------------------------------------------------
        .filter(
            col("train_id").isNotNull()
        )
    )


# ============================================================
# SILVER - TRAINS
# ============================================================

dp.create_streaming_table(
    name="motordatabricks.silver.trains",

    comment="Silver railway trains table with cleaned and standardized CDC data",

    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)


dp.create_auto_cdc_flow(
    flow_name="trains_cdc",

    target="motordatabricks.silver.trains",

    source="trains_cdf",

    keys=[
        "train_id"
    ],

    sequence_by=col("_commit_version"),

    apply_as_deletes=expr(
        "_change_type = 'delete'"
    ),

    except_column_list=[
        "_change_type",
        "_commit_version",
        "_commit_timestamp"
    ],

    stored_as_scd_type=1
)


# ============================================================
# 2. SCHEDULES CDF + SILVER TRANSFORMATION
# ============================================================

@dp.view(name="schedules_cdf")
def schedules_cdf():

    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(BRONZE_SCHEDULES)

        # ----------------------------------------------------
        # String cleaning
        # ----------------------------------------------------
        .withColumn(
            "source_station",
            clean_string("source_station")
        )

        .withColumn(
            "destination_station",
            clean_string("destination_station")
        )

        .withColumn(
            "schedule_status",
            clean_string("schedule_status")
        )

        # ----------------------------------------------------
        # Standardization
        # ----------------------------------------------------
        .withColumn(
            "source_station",
            initcap(col("source_station"))
        )

        .withColumn(
            "destination_station",
            initcap(col("destination_station"))
        )

        .withColumn(
            "schedule_status",
            upper(col("schedule_status"))
        )

        # ----------------------------------------------------
        # Explicit datatype conversion
        # ----------------------------------------------------
        .withColumn(
            "schedule_id",
            col("schedule_id").cast("int")
        )

        .withColumn(
            "train_id",
            col("train_id").cast("int")
        )

        .withColumn(
            "departure_time",
            to_timestamp(col("departure_time"))
        )

        .withColumn(
            "arrival_time",
            to_timestamp(col("arrival_time"))
        )

        .withColumn(
            "updated_at",
            to_timestamp(col("updated_at"))
        )

        # ----------------------------------------------------
        # Data quality
        # ----------------------------------------------------
        .filter(
            col("schedule_id").isNotNull()
        )

        .filter(
            col("train_id").isNotNull()
        )
    )


# ============================================================
# SILVER - SCHEDULES
# ============================================================

dp.create_streaming_table(
    name="motordatabricks.silver.schedules",

    comment="Silver railway schedules table with cleaned and standardized CDC data",

    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)


dp.create_auto_cdc_flow(
    flow_name="schedules_cdc",

    target="motordatabricks.silver.schedules",

    source="schedules_cdf",

    keys=[
        "schedule_id"
    ],

    sequence_by=col("_commit_version"),

    apply_as_deletes=expr(
        "_change_type = 'delete'"
    ),

    except_column_list=[
        "_change_type",
        "_commit_version",
        "_commit_timestamp"
    ],

    stored_as_scd_type=1
)


# ============================================================
# 3. MAINTENANCE WORKORDERS CDF + SILVER TRANSFORMATION
# ============================================================

@dp.view(name="maintenance_workorders_cdf")
def maintenance_workorders_cdf():

    return (
        spark.readStream
        .option("readChangeFeed", "true")
        .table(BRONZE_WORKORDERS)

        # ----------------------------------------------------
        # String cleaning
        # ----------------------------------------------------
        .withColumn(
            "maintenance_type",
            clean_string("maintenance_type")
        )

        .withColumn(
            "description",
            clean_string("description")
        )

        .withColumn(
            "priority",
            clean_string("priority")
        )

        .withColumn(
            "status",
            clean_string("status")
        )

        .withColumn(
            "technician",
            clean_string("technician")
        )

        # ----------------------------------------------------
        # Standardization
        # ----------------------------------------------------
        .withColumn(
            "maintenance_type",
            initcap(col("maintenance_type"))
        )

        .withColumn(
            "priority",
            upper(col("priority"))
        )

        .withColumn(
            "status",
            upper(col("status"))
        )

        .withColumn(
            "technician",
            initcap(col("technician"))
        )

        # ----------------------------------------------------
        # Explicit datatype conversion
        # ----------------------------------------------------
        .withColumn(
            "workorder_id",
            col("workorder_id").cast("int")
        )

        .withColumn(
            "train_id",
            col("train_id").cast("int")
        )

        .withColumn(
            "scheduled_date",
            to_date(col("scheduled_date"))
        )

        .withColumn(
            "completed_date",
            to_date(col("completed_date"))
        )

        .withColumn(
            "updated_at",
            to_timestamp(col("updated_at"))
        )

        # ----------------------------------------------------
        # Data quality
        # ----------------------------------------------------
        .filter(
            col("workorder_id").isNotNull()
        )

        .filter(
            col("train_id").isNotNull()
        )
    )


# ============================================================
# SILVER - MAINTENANCE WORKORDERS
# ============================================================

dp.create_streaming_table(
    name="motordatabricks.silver.maintenance_workorders",

    comment="Silver railway maintenance workorders table with cleaned and standardized CDC data",

    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)


dp.create_auto_cdc_flow(
    flow_name="maintenance_workorders_cdc",

    target="motordatabricks.silver.maintenance_workorders",

    source="maintenance_workorders_cdf",

    keys=[
        "workorder_id"
    ],

    sequence_by=col("_commit_version"),

    apply_as_deletes=expr(
        "_change_type = 'delete'"
    ),

    except_column_list=[
        "_change_type",
        "_commit_version",
        "_commit_timestamp"
    ],

    stored_as_scd_type=1
)