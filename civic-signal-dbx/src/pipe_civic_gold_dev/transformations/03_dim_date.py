from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="dim_date",
    comment=(
        "Reusable calendar dimension for Civic Signal analytical models, "
        "covering January 2025 through December 2030."
    ),
    schema="""
        date_key INT COMMENT 'Deterministic calendar key in YYYYMMDD format.',
        date DATE COMMENT 'Calendar date.',
        year INT COMMENT 'Calendar year.',
        quarter INT COMMENT 'Calendar quarter number from 1 through 4.',
        month INT COMMENT 'Calendar month number from 1 through 12.',
        month_name STRING COMMENT 'Full calendar month name.',
        week_of_year INT COMMENT 'Week number within the calendar year.',
        day_of_month INT COMMENT 'Day number within the month.',
        day_name STRING COMMENT 'Full weekday name.',
        is_weekend BOOLEAN COMMENT 'True when the date falls on Saturday or Sunday.'
    """
)
@dp.expect_or_fail("date_key_required", "date_key IS NOT NULL")
def dim_date():

    dates = (
        spark.sql("""
            SELECT explode(
                sequence(
                    DATE('2025-01-01'),
                    DATE('2030-12-31'),
                    INTERVAL 1 DAY
                )
            ) AS date
        """)
    )

    return (
        dates
        .select(
            F.date_format("date", "yyyyMMdd").cast("int").alias("date_key"),
            F.col("date"),
            F.year("date").alias("year"),
            F.quarter("date").alias("quarter"),
            F.month("date").alias("month"),
            F.date_format("date", "MMMM").alias("month_name"),
            F.weekofyear("date").alias("week_of_year"),
            F.dayofmonth("date").alias("day_of_month"),
            F.date_format("date", "EEEE").alias("day_name"),
            F.dayofweek("date").isin(1, 7).alias("is_weekend")
        )
    )