from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.materialized_view(
    name="fact_opportunity",
    comment=(
        "Current-state synthetic procurement opportunity fact table for Civic Signal. "
        "One row represents one valid procurement opportunity."
    ),
    schema="""
        opportunity_id STRING COMMENT 'Stable synthetic opportunity business key and fact grain.',
        opportunity_title STRING COMMENT 'Business-friendly title of the synthetic procurement opportunity.',
        opportunity_country STRING COMMENT 'Country associated with the procurement opportunity.',
        buyer_id STRING COMMENT 'Buyer dimension key referencing dim_buyer.',
        category_code STRING COMMENT 'Category dimension key referencing dim_category.',
        published_date_key INT COMMENT 'Calendar key referencing dim_date for the publication date.',
        closing_date_key INT COMMENT 'Calendar key referencing dim_date for the closing date when available.',
        procurement_method STRING COMMENT 'Procurement procedure or sourcing method.',
        source_system STRING COMMENT 'Synthetic source system from which the opportunity originated.',
        status STRING COMMENT 'Current opportunity lifecycle status.',
        estimated_value DECIMAL(18,2) COMMENT 'Synthetic indicative opportunity value; values must not be aggregated across different currencies.',
        currency STRING COMMENT 'Currency code explicitly provided by the synthetic source.',
        opportunity_count INT COMMENT 'Additive row-count measure with value 1 for each opportunity.',
        published_at TIMESTAMP COMMENT 'Opportunity publication timestamp.',
        closing_at TIMESTAMP COMMENT 'Opportunity closing timestamp.',
        updated_at TIMESTAMP COMMENT 'Timestamp of the latest accepted source update.'
    """
)
@dp.expect_or_fail(
    "fact_grain_required",
    """
    opportunity_id IS NOT NULL
    AND buyer_id IS NOT NULL
    AND category_code IS NOT NULL
    AND published_date_key IS NOT NULL
    """
)
@dp.expect(
    "non_negative_estimated_value",
    "estimated_value IS NULL OR estimated_value >= 0"
)
def fact_opportunity():

    source = spark.read.table(
        "civic_signal_dbx_dev.silver.opportunities"
    )

    return (
        source
        .select(
            "opportunity_id",
            "opportunity_title",
            F.col("country").alias("opportunity_country"),
            "buyer_id",
            "category_code",

            F.date_format(
                F.to_date("published_at"),
                "yyyyMMdd"
            ).cast("int").alias("published_date_key"),

            F.when(
                F.col("closing_at").isNotNull(),
                F.date_format(
                    F.to_date("closing_at"),
                    "yyyyMMdd"
                ).cast("int")
            ).alias("closing_date_key"),

            "procurement_method",
            "source_system",
            "status",
            "estimated_value",
            "currency",

            F.lit(1).cast("int").alias("opportunity_count"),

            "published_at",
            "closing_at",
            "updated_at"
        )
    )