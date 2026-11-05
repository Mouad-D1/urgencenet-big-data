"""Schemas explicites des donnees, partages entre le batch (Partie 3) et le streaming (Parties 4 et 5)."""
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

EVENT_SCHEMA = StructType([
    StructField("visit_id", StringType()),
    StructField("patient_id", StringType()),
    StructField("facility_id", StringType()),
    StructField("city", StringType()),
    StructField("status", StringType()),
    StructField("priority", IntegerType()),
    StructField("wait_minutes", IntegerType()),
    StructField("department", StringType()),
    StructField("desk_id", StringType()),
    StructField("transfer_to", StringType()),
    StructField("event_time", TimestampType()),
])

FACILITY_SCHEMA = StructType([
    StructField("facility_id", StringType()),
    StructField("name", StringType()),
    StructField("city", StringType()),
    StructField("hospital_group", StringType()),
    StructField("type", StringType()),
    StructField("beds", IntegerType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
])
