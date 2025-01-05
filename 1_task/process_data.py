import logging
import os
from dataclasses import dataclass
from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("SparkKafkaProcessor")

@dataclass
class KafkaSettings:
    servers: List[str]
    user: str
    password: str
    protocol: str
    mechanism: str

    @property
    def jaas_config(self) -> str:
        return (
            "org.apache.kafka.common.security.plain.PlainLoginModule required "
            f'username="{self.user}" password="{self.password}";'
        )

    @classmethod
    def from_env(cls) -> 'KafkaSettings':
        return cls(
            servers=[os.getenv("KAFKA_SERVERS", "77.81.230.104:9092")],
            user=os.getenv("KAFKA_USER", "admin"),
            password=os.getenv("KAFKA_PASS", "VawEzo1ikLtrA8Ug8THa"),
            protocol=os.getenv("KAFKA_PROTOCOL", "SASL_PLAINTEXT"),
            mechanism=os.getenv("KAFKA_MECHANISM", "PLAIN")
        )

@dataclass
class DatabaseSettings:
    host: str
    port: int
    name: str
    username: str
    password: str
    driver: str = "com.mysql.cj.jdbc.Driver"

    @property
    def jdbc_url(self) -> str:
        return f"jdbc:mysql://{self.host}:{self.port}/{self.name}"

    @classmethod
    def from_env(cls) -> 'DatabaseSettings':
        return cls(
            host=os.getenv("DB_HOST", "217.61.57.46"),
            port=int(os.getenv("DB_PORT", "3306")),
            name=os.getenv("DB_NAME", "olympic_dataset"),
            username=os.getenv("DB_USER", "neo_data_admin"),
            password=os.getenv("DB_PASS", "Proyahaxuqithab9oplp")
        )

class SparkProcessor:
    def __init__(self, kafka: KafkaSettings, database: DatabaseSettings, checkpoint_dir: str = "checkpoint"):
        self.kafka = kafka
        self.database = database
        self.checkpoint_dir = checkpoint_dir
        self.spark = self._initialize_spark_session()
        self.schema = self._define_schema()

    def _initialize_spark_session(self) -> SparkSession:
        jar_path = os.path.abspath("/1_task/mysql-connector-j-8.0.32.jar")
        if not os.path.exists(jar_path):
            raise FileNotFoundError(f"MySQL Connector JAR not found at {jar_path}")

        return (
            SparkSession.builder
            .config("spark.jars", jar_path)
            .config("spark.driver.extraClassPath", jar_path)
            .config("spark.executor.extraClassPath", jar_path)
            .appName("KafkaToSparkProcessor")
            .master("local[*]")
            .getOrCreate()
        )

    def _define_schema(self) -> StructType:
        return StructType([
            StructField("athlete_id", IntegerType(), True),
            StructField("sport", StringType(), True),
            StructField("medal", StringType(), True),
            StructField("timestamp", StringType(), True),
        ])

    def read_from_mysql(self, table: str, partition_column: str) -> DataFrame:
        try:
            return (
                self.spark.read.format("jdbc")
                .option("url", self.database.jdbc_url)
                .option("driver", self.database.driver)
                .option("dbtable", table)
                .option("user", self.database.username)
                .option("password", self.database.password)
                .option("partitionColumn", partition_column)
                .option("lowerBound", 1)
                .option("upperBound", 1000000)
                .option("numPartitions", 10)
                .load()
            )
        except Exception as e:
            logger.error(f"Failed to read from MySQL table {table}: {e}")
            raise

    def write_to_mysql(self, df: DataFrame, table: str):
        try:
            (df.write
             .format("jdbc")
             .option("url", self.database.jdbc_url)
             .option("driver", self.database.driver)
             .option("dbtable", table)
             .option("user", self.database.username)
             .option("password", self.database.password)
             .mode("append")
             .save())
            logger.info(f"Data successfully written to MySQL table {table}")
        except Exception as e:
            logger.error(f"Failed to write to MySQL table {table}: {e}")
            raise

    def write_to_kafka(self, df: DataFrame, topic: str):
        try:
            (df.selectExpr("CAST(NULL AS STRING) AS key", "to_json(struct(*)) AS value")
             .write
             .format("kafka")
             .option("kafka.bootstrap.servers", ",".join(self.kafka.servers))
             .option("kafka.security.protocol", self.kafka.protocol)
             .option("kafka.sasl.mechanism", self.kafka.mechanism)
             .option("kafka.sasl.jaas.config", self.kafka.jaas_config)
             .option("topic", topic)
             .save())
        except Exception as e:
            logger.error(f"Failed to write to Kafka topic {topic}: {e}")
            raise

    def process_stream(self):
        try:
            data = self.read_from_mysql("athlete_bio", "athlete_id")
            self.write_to_kafka(data, "kilaru_athlete_event_results")
            self.write_to_mysql(data, "kilaru_enriched_athlete_avg")
        except Exception as e:
            logger.error(f"Stream processing failed: {e}")
            raise

def main():
    try:
        kafka_settings = KafkaSettings.from_env()
        db_settings = DatabaseSettings.from_env()
        processor = SparkProcessor(kafka_settings, db_settings)
        processor.process_stream()
    except Exception as e:
        logger.error(f"Application encountered an error: {e}")

if __name__ == "__main__":
    main()
