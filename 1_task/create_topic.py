import os
import logging
from typing import Dict, List, Optional
from enum import Enum
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError
from dataclasses import dataclass
from colorama import Fore, Style, init

# Initialize colorama
init(autoreset=True)

class LogFormatter(logging.Formatter):
    LEVEL_COLORS = {
        logging.WARNING: Fore.YELLOW,
        logging.ERROR: Fore.RED,
        logging.INFO: Fore.GREEN,
        logging.DEBUG: Fore.BLUE,
    }

    def format(self, record):
        color = self.LEVEL_COLORS.get(record.levelno, '')
        record.msg = f"{color}{record.msg}{Style.RESET_ALL}"
        return super().format(record)

# Configure logging
logger = logging.getLogger(__name__)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(LogFormatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(stream_handler)
logger.setLevel(logging.INFO)

class Protocol(Enum):
    PLAINTEXT = "PLAINTEXT"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    SSL = "SSL"
    SASL_SSL = "SASL_SSL"

@dataclass
class KafkaSettings:
    brokers: str
    user: str
    password: str
    protocol: Protocol
    mechanism: str
    prefix: str

    @staticmethod
    def load_from_env() -> 'KafkaSettings':
        return KafkaSettings(
            brokers=os.getenv("KAFKA_BROKERS", "77.81.230.104:9092"),
            user=os.getenv("KAFKA_USER", "admin"),
            password=os.getenv("KAFKA_PASS", "VawEzo1ikLtrA8Ug8THa"),
            protocol=Protocol(os.getenv("KAFKA_PROTOCOL", "SASL_PLAINTEXT")),
            mechanism=os.getenv("KAFKA_MECHANISM", "PLAIN"),
            prefix=os.getenv("KAFKA_PREFIX", "kilaru")
        )

    def admin_config(self) -> Dict[str, str]:
        return {
            "bootstrap_servers": self.brokers,
            "security_protocol": self.protocol.value,
            "sasl_mechanism": self.mechanism,
            "sasl_plain_username": self.user,
            "sasl_plain_password": self.password,
        }

class KafkaManager:
    def __init__(self, settings: KafkaSettings):
        self.settings = settings
        self.client = KafkaAdminClient(**settings.admin_config())

    def shutdown(self):
        if self.client:
            self.client.close()

    def remove_topics(self, topics: List[str]):
        try:
            available_topics = self.client.list_topics()
            topics_to_remove = [t for t in topics if t in available_topics]

            if topics_to_remove:
                self.client.delete_topics(topics_to_remove)
                for topic in topics_to_remove:
                    logger.info(f"Successfully removed topic: {topic}")
            else:
                logger.info("No topics to remove.")
        except KafkaError as error:
            logger.error(f"Error while removing topics: {error}")
        except Exception as ex:
            logger.error(f"Unexpected error during topic removal: {ex}")

    def add_topics(self, topics: Dict[str, str]):
        try:
            new_topics = [
                NewTopic(name=topic, num_partitions=1, replication_factor=1)
                for topic in topics
            ]

            self.client.create_topics(new_topics)
            for topic in topics:
                logger.info(f"Successfully added topic: {topic}")
        except KafkaError as error:
            logger.error(f"Error while adding topics: {error}")
        except Exception as ex:
            logger.error(f"Unexpected error during topic creation: {ex}")

    def fetch_topics(self) -> Optional[List[str]]:
        try:
            topics = self.client.list_topics()
            matching = [t for t in topics if self.settings.prefix in t]

            for topic in matching:
                logger.info(f"Topic found: {topic}")
            return matching
        except KafkaError as error:
            logger.error(f"Error while fetching topics: {error}")
        except Exception as ex:
            logger.error(f"Unexpected error during topic fetching: {ex}")
        return None

def run():
    manager = None
    try:
        settings = KafkaSettings.load_from_env()

        topic_definitions = {
            f"{settings.prefix}_results": "Results topic",
            f"{settings.prefix}_metrics": "Metrics topic",
        }

        manager = KafkaManager(settings)
        logger.info("Managing Kafka topics...")

        manager.remove_topics(list(topic_definitions.keys()))
        manager.add_topics(topic_definitions)
        manager.fetch_topics()

    except Exception as ex:
        logger.error(f"Error during Kafka operations: {ex}")
        raise
    finally:
        if manager:
            manager.shutdown()

if __name__ == "__main__":
    run()
