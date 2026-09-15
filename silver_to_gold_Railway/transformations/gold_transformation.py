from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    trim,
    upper,
    initcap,
    lower,
    when,
    date_format,
    datediff,
    unix_timestamp,
    round,
    dayofweek,
    dayofmonth,
    weekofyear,
    month,
    year,
    quarter,
    explode,
    sequence,
    min as spark_min,
    max as spark_max,
    lit,
    expr
)


# ============================================================
# CONFIGURATION
# ============================================================

SILVER_TRAINS = "motordatabricks.silver.trains"
SILVER_SCHEDULES = "motordatabricks.silver.schedules"
SILVER_WORKORDERS = "motordatabricks.silver.maintenance_workorders"

GOLD_SCHEMA = "motordatabricks.gold"


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


def station_key(column_name):
    """
    Create a consistent natural key for stations.

    We use the normalized station name because the source
    system does not provide a station_id.
    """
    return upper(
        trim(
            col(column_name)
        )
    )


def maintenance_type_key(column_name):
    """
    Create a consistent natural key for maintenance type.
    """
    return upper(
        trim(
            col(column_name)
        )
    )


# ============================================================
# ============================================================
# DIMENSION 1 - TRAIN
# ============================================================
# ============================================================

@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.dim_train",
    comment="Gold train dimension containing the current state of railway trains"
)
def dim_train():

    return (
        spark.read.table(SILVER_TRAINS)

        .filter(
            col("train_id").isNotNull()
        )

        .select(
            col("train_id").alias("train_key"),

            col("train_number"),

            col("train_name"),

            col("train_type"),

            col("current_status"),

            col("current_station"),

            col("updated_at")
        )

        .dropDuplicates(["train_key"])
    )


# ============================================================
# ============================================================
# DIMENSION 2 - STATION
# ============================================================
# ============================================================

@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.dim_station",
    comment="Gold station dimension derived from railway schedules and train locations"
)
def dim_station():

    # Stations from schedules - source station
    schedule_source = (
        spark.read.table(SILVER_SCHEDULES)
        .select(
            station_key("source_station").alias("station_key"),
            initcap(
                trim(
                    col("source_station")
                )
            ).alias("station_name")
        )
        .filter(
            col("station_key").isNotNull()
        )
    )

    # Stations from schedules - destination station
    schedule_destination = (
        spark.read.table(SILVER_SCHEDULES)
        .select(
            station_key("destination_station").alias("station_key"),
            initcap(
                trim(
                    col("destination_station")
                )
            ).alias("station_name")
        )
        .filter(
            col("station_key").isNotNull()
        )
    )

    # Current train stations
    train_station = (
        spark.read.table(SILVER_TRAINS)
        .select(
            station_key("current_station").alias("station_key"),
            initcap(
                trim(
                    col("current_station")
                )
            ).alias("station_name")
        )
        .filter(
            col("station_key").isNotNull()
        )
    )

    return (
        schedule_source

        .unionByName(schedule_destination)

        .unionByName(train_station)

        .dropDuplicates(["station_key"])

        .select(
            col("station_key"),
            col("station_name")
        )
    )


# ============================================================
# ============================================================
# DIMENSION 3 - MAINTENANCE TYPE
# ============================================================
# ============================================================

@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.dim_maintenance_type",
    comment="Gold maintenance type dimension"
)
def dim_maintenance_type():

    return (
        spark.read.table(SILVER_WORKORDERS)

        .select(
            maintenance_type_key("maintenance_type").alias(
                "maintenance_type_key"
            ),

            initcap(
                trim(
                    col("maintenance_type")
                )
            ).alias("maintenance_type")
        )

        .filter(
            col("maintenance_type_key").isNotNull()
        )

        .dropDuplicates(["maintenance_type_key"])
    )


# ============================================================
# ============================================================
# DIMENSION 4 - DATE
# ============================================================
# ============================================================

@dp.materialized_view(
    name=f"{GOLD_SCHEMA}.dim_date",
    comment="Gold calendar/date dimension"
)
def dim_date():

    # --------------------------------------------------------
    # Collect all relevant dates from Silver
    # --------------------------------------------------------

    schedule_dates = (
        spark.read.table(SILVER_SCHEDULES)
        .select(
            col("departure_time").cast("date").alias("date_value")
        )
        .unionByName(
            spark.read.table(SILVER_SCHEDULES)
            .select(
                col("arrival_time").cast("date").alias("date_value")
            )
        )
    )

    maintenance_dates = (
        spark.read.table(SILVER_WORKORDERS)
        .select(
            col("scheduled_date").alias("date_value")
        )
        .unionByName(
            spark.read.table(SILVER_WORKORDERS)
            .select(
                col("completed_date").alias("date_value")
            )
        )
    )

    all_dates = (
        schedule_dates

        .unionByName(maintenance_dates)

        .filter(
            col("date_value").isNotNull()
        )
    )

    # --------------------------------------------------------
    # Find minimum and maximum date
    # --------------------------------------------------------

    date_range = (
        all_dates
        .agg(
            spark_min("date_value").alias("min_date"),
            spark_max("date_value").alias("max_date")
        )
    )

    # --------------------------------------------------------
    # Generate every date between min and max
    # --------------------------------------------------------

    return (
        date_range

        .select(
            explode(
                sequence(
                    col("min_date"),
                    col("max_date"),
                    expr("interval 1 day")
                )
            ).alias("full_date")
        )

        .select(
            date_format(
                col("full_date"),
                "yyyyMMdd"
            ).cast("int").alias("date_key"),

            col("full_date"),

            dayofmonth(
                col("full_date")
            ).alias("day"),

            month(
                col("full_date")
            ).alias("month"),

            year(
                col("full_date")
            ).alias("year"),

            quarter(
                col("full_date")
            ).alias("quarter"),

            weekofyear(
                col("full_date")
            ).alias("week_of_year"),

            date_format(
                col("full_date"),
                "EEEE"
            ).alias("day_name"),

            date_format(
                col("full_date"),
                "MMMM"
            ).alias("month_name"),

            when(
                dayofweek(col("full_date")).isin(1, 7),
                True
            )
            .otherwise(False)
            .alias("is_weekend")
        )
    )


# ============================================================
# ============================================================
# SILVER CDF SOURCE - SCHEDULES
# ============================================================
# ============================================================

@dp.temporary_view(
    name="schedules_gold_cdf"
)
def schedules_gold_cdf():

    return (
        spark.readStream

        .option(
            "readChangeFeed",
            "true"
        )

        .table(SILVER_SCHEDULES)

        # ----------------------------------------------------
        # Business transformations
        # ----------------------------------------------------

        .withColumn(
            "source_station_key",
            station_key("source_station")
        )

        .withColumn(
            "destination_station_key",
            station_key("destination_station")
        )

        .withColumn(
            "departure_date_key",
            date_format(
                col("departure_time"),
                "yyyyMMdd"
            ).cast("int")
        )

        .withColumn(
            "arrival_date_key",
            date_format(
                col("arrival_time"),
                "yyyyMMdd"
            ).cast("int")
        )

        .withColumn(
            "schedule_status",
            upper(
                trim(
                    col("schedule_status")
                )
            )
        )

        # ----------------------------------------------------
        # Derived business metric
        # ----------------------------------------------------

        .withColumn(
            "scheduled_duration_minutes",
            when(
                col("arrival_time").isNotNull()
                &
                col("departure_time").isNotNull(),

                round(
                    (
                        unix_timestamp(col("arrival_time"))
                        -
                        unix_timestamp(col("departure_time"))
                    ) / 60
                ).cast("int")
            )
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

        .filter(
            col("departure_time").isNotNull()
        )

        .filter(
            col("arrival_time").isNotNull()
        )
    )


# ============================================================
# ============================================================
# FACT 1 - SCHEDULE
# ============================================================
# ============================================================

dp.create_streaming_table(
    name=f"{GOLD_SCHEMA}.fact_schedule",

    comment="Gold railway schedule fact table maintained incrementally from Silver CDF",

    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)


dp.create_auto_cdc_flow(

    flow_name="fact_schedule_cdc",

    target=f"{GOLD_SCHEMA}.fact_schedule",

    source="schedules_gold_cdf",

    # Natural/business key
    keys=[
        "schedule_id"
    ],

    # Silver Delta commit ordering
    sequence_by=col(
        "_commit_version"
    ),

    # Handle deletes from Silver
    apply_as_deletes=expr(
        "_change_type = 'delete'"
    ),

    # Remove Silver CDF metadata
    except_column_list=[
        "_change_type",
        "_commit_version",
        "_commit_timestamp"
    ],

    # Current-state fact
    stored_as_scd_type=1
)


# ============================================================
# ============================================================
# SILVER CDF SOURCE - MAINTENANCE WORKORDERS
# ============================================================
# ============================================================

@dp.temporary_view(
    name="maintenance_gold_cdf"
)
def maintenance_gold_cdf():

    return (
        spark.readStream

        .option(
            "readChangeFeed",
            "true"
        )

        .table(SILVER_WORKORDERS)

        # ----------------------------------------------------
        # Dimension keys
        # ----------------------------------------------------

        .withColumn(
            "maintenance_type_key",
            maintenance_type_key("maintenance_type")
        )

        .withColumn(
            "scheduled_date_key",
            when(
                col("scheduled_date").isNotNull(),

                date_format(
                    col("scheduled_date"),
                    "yyyyMMdd"
                ).cast("int")
            )
        )

        .withColumn(
            "completed_date_key",
            when(
                col("completed_date").isNotNull(),

                date_format(
                    col("completed_date"),
                    "yyyyMMdd"
                ).cast("int")
            )
        )

        # ----------------------------------------------------
        # Standardize business attributes
        # ----------------------------------------------------

        .withColumn(
            "priority",
            upper(
                trim(
                    col("priority")
                )
            )
        )

        .withColumn(
            "status",
            upper(
                trim(
                    col("status")
                )
            )
        )

        # ----------------------------------------------------
        # Derived business metrics
        # ----------------------------------------------------

        .withColumn(
            "maintenance_duration_days",
            when(
                col("scheduled_date").isNotNull()
                &
                col("completed_date").isNotNull(),

                datediff(
                    col("completed_date"),
                    col("scheduled_date")
                )
            )
        )

        .withColumn(
            "is_completed",
            when(
                col("completed_date").isNotNull(),
                True
            )
            .otherwise(False)
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
# ============================================================
# FACT 2 - MAINTENANCE
# ============================================================
# ============================================================

dp.create_streaming_table(
    name=f"{GOLD_SCHEMA}.fact_maintenance",

    comment="Gold railway maintenance fact table maintained incrementally from Silver CDF",

    table_properties={
        "delta.enableChangeDataFeed": "true"
    }
)


dp.create_auto_cdc_flow(

    flow_name="fact_maintenance_cdc",

    target=f"{GOLD_SCHEMA}.fact_maintenance",

    source="maintenance_gold_cdf",

    # Natural/business key
    keys=[
        "workorder_id"
    ],

    # Silver Delta commit ordering
    sequence_by=col(
        "_commit_version"
    ),

    # Handle deletes
    apply_as_deletes=expr(
        "_change_type = 'delete'"
    ),

    # Remove CDF metadata
    except_column_list=[
        "_change_type",
        "_commit_version",
        "_commit_timestamp"
    ],

    # Current-state fact
    stored_as_scd_type=1
)