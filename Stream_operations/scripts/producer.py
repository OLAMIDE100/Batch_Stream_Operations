import csv
from time import sleep

from confluent_kafka import avro
from confluent_kafka.avro import AvroProducer


def load_avro_schema_from_file():
    key_schema = avro.load("taxi_ride_key.avsc")
    value_schema = avro.load("taxi_ride_value.avsc")

    return key_schema, value_schema


def send_record():
    key_schema, value_schema = load_avro_schema_from_file()

    producer_config = {
        "bootstrap.servers": "localhost:9092",
        "schema.registry.url": "http://localhost:8081",
        "acks": "1",
    }

    producer = AvroProducer(
        producer_config,
        default_key_schema=key_schema,
        default_value_schema=value_schema,
    )

    file = open(
        r"C:\Users\ADESOBA OLAMIDE\Desktop\BigSpark_Solution\Batch_operations\spark\resources\data\output\result.csv"
    )

    csvreader = csv.reader(file)
    header = next(csvreader)
    for row in csvreader:
        key = {"Store": str(row[0])}
        value = {
            "Store": str(row[0]),
            "quantity": str(row[1]),
            "Revenue": str(row[2]),
            "year": str(row[3]),
            "brand": str(row[4]),
        }

        try:
            producer.produce(topic="result", key=key, value=value)
        except Exception as e:
            print(f"Exception while producing record value - {value}: {e}")
        else:
            print(f"Successfully producing record value - {value}")

        producer.flush()
        sleep(1)


if __name__ == "__main__":
    send_record()
