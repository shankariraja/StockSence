from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, stddev
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, DateType
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.sql.window import Window

def create_spark_session():
    return SparkSession.builder \
        .appName("StockDataProcessing") \
        .getOrCreate()

def define_schema():
    return StructType([
        StructField("date", StringType(), True),
        StructField("open", DoubleType(), True),
        StructField("high", DoubleType(), True),
        StructField("low", DoubleType(), True),
        StructField("close", DoubleType(), True),
        StructField("volume", LongType(), True),
        StructField("company_info", StructType([
            StructField("symbol", StringType(), True),
            StructField("longName", StringType(), True),
            StructField("longBusinessSummary", StringType(), True),
            StructField("sector", StringType(), True),
            StructField("industry", StringType(), True),
            StructField("totalRevenue", DoubleType(), True),
            StructField("marketCap", DoubleType(), True),
            StructField("bookValue", DoubleType(), True),
            StructField("priceToBook", DoubleType(), True),
            StructField("dividendYield", DoubleType(), True),
            StructField("returnOnEquity", DoubleType(), True),
            StructField("priceToSalesTrailing12Months", DoubleType(), True),
            StructField("recommendationKey", StringType(), True)
        ]), True)
    ])

def read_from_kafka(spark, bootstrap_servers, topic):
    return spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", bootstrap_servers) \
        .option("subscribe", topic) \
        .load()

def preprocess_data(df, schema):
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")
    parsed_df = parsed_df.withColumn("date", col("date").cast(DateType()))

    filled_df = parsed_df \
        .na.fill(0, ["open", "high", "low", "close", "volume"]) \
        .na.fill("Unknown", ["company_info.sector", "company_info.industry", "company_info.recommendationKey"])

    window_spec = Window.partitionBy("company_info.symbol").orderBy("date")
    processed_df = filled_df \
        .withColumn("daily_return", (col("close") - col("open")) / col("open")) \
        .withColumn("price_change", col("close") - col("open")) \
        .withColumn("price_change_percentage", (col("close") - col("open")) / col("open") * 100) \
        .withColumn("previous_close", col("close").lag(1).over(window_spec)) \
        .withColumn("volume_change", (col("volume") - col("volume").lag(1).over(window_spec)) / col("volume").lag(1).over(window_spec) * 100)

    processed_df = processed_df \
        .withColumn("MA5", avg("close").over(window_spec.rowsBetween(-4, 0))) \
        .withColumn("MA20", avg("close").over(window_spec.rowsBetween(-19, 0)))

    processed_df = processed_df \
        .withColumn("volatility", stddev("daily_return").over(window_spec.rowsBetween(-19, 0)))

    processed_df = processed_df \
        .withColumn("dividendYield", col("company_info.dividendYield") * 100) \
        .withColumn("returnOnEquity", col("company_info.returnOnEquity") * 100)

    feature_cols = ["open", "high", "low", "close", "volume", "daily_return", "MA5", "MA20", "volatility"]
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")
    featured_df = assembler.transform(processed_df)

    scaler = StandardScaler(inputCol="features", outputCol="scaled_features")
    scaled_df = scaler.fit(featured_df).transform(featured_df)

    return scaled_df.select(
        "date", "company_info.symbol", "open", "high", "low", "close", "volume",
        "daily_return", "price_change", "price_change_percentage", "volume_change",
        "MA5", "MA20", "volatility", "scaled_features",
        "company_info.sector", "company_info.industry", "company_info.totalRevenue",
        "company_info.marketCap", "company_info.bookValue", "company_info.priceToBook",
        "dividendYield", "returnOnEquity", "company_info.priceToSalesTrailing12Months",
        "company_info.recommendationKey"
    )

def write_to_console(df):
    return df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()

def main():
    spark = create_spark_session()
    schema = define_schema()
    
    df = read_from_kafka(spark, "localhost:9092", "stock_market_data")
    processed_df = preprocess_data(df, schema)
    
    query = write_to_console(processed_df)
    query.awaitTermination()

if __name__ == "__main__":
    main()