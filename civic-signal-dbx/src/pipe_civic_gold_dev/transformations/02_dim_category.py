from pyspark import pipelines as dp

@dp.materialized_view(
    name="dim_category",
    comment=(
        "Synthetic procurement category dimension for Civic Signal. "
        "One row represents one governed procurement category."
    ),
    schema="""
        category_code STRING COMMENT 'Stable synthetic procurement category key.',
        category_name STRING COMMENT 'Business-friendly procurement category name.',
        category_group STRING COMMENT 'Higher-level grouping of related procurement categories.'
    """
)
@dp.expect_or_fail("category_key_required", "category_code IS NOT NULL")
def dim_category():
    return (
        spark.read
        .table("civic_signal_dbx_dev.silver.categories")
        .select(
            "category_code",
            "category_name",
            "category_group"
        )
    )