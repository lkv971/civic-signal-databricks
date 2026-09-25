from pyspark import pipelines as dp

@dp.materialized_view(
    name="dim_buyer",
    comment=(
        "Current-state synthetic public buyer dimension for Civic Signal. "
        "One row represents one buyer."
    ),
    schema="""
        buyer_id STRING COMMENT 'Stable synthetic buyer business key.',
        buyer_name STRING COMMENT 'Current synthetic buyer display name.',
        buyer_type STRING COMMENT 'Buyer organization type.',
        country STRING COMMENT 'Country associated with the buyer.',
        region STRING COMMENT 'Geographic region associated with the buyer.',
        source_system STRING COMMENT 'Synthetic source system supplying the buyer record.',
        updated_at TIMESTAMP COMMENT 'Timestamp of the latest accepted buyer update.'
    """
)
@dp.expect_or_fail("buyer_key_required", "buyer_id IS NOT NULL")
def dim_buyer():
    return (
        spark.read
        .table("civic_signal_dbx_dev.silver.buyers")
        .select(
            "buyer_id",
            "buyer_name",
            "buyer_type",
            "country",
            "region",
            "source_system",
            "updated_at"
        )
    )