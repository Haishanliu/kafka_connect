from dataclasses import dataclass
import datetime
import argparse
from functools import cached_property
import io
import os
from kafka import KafkaConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
import avro.schema
import avro.io
import csv

@dataclass(frozen=True)
class _AvroDS:
    schema_registry_url: str
    topic: str

    @cached_property
    def _avro_deserializer(self) -> avro.schema.Schema:
        key = os.environ['SR_USERNAME']
        pw = os.environ['SR_PASSWORD']
        sr_conf = {'url': self.schema_registry_url, "basic.auth.user.info":f"{key}:{pw}"}
        schema_registry_client = SchemaRegistryClient(sr_conf)
        schema = schema_registry_client.get_latest_version(f'{self.topic}-value').schema
        return avro.schema.parse(schema.schema_str)

    @cached_property
    def datum_reader(self) -> avro.io.DatumReader:
        return avro.io.DatumReader(self._avro_deserializer)

    def decode_value(self, data: bytes) -> dict:
        try:
            bytes_reader = io.BytesIO(data[5:])
            decoder = avro.io.BinaryDecoder(bytes_reader)
            return self.datum_reader.read(decoder)
        except Exception as e:
            return {'error': e}

def _dump_message(msg):
    target_keys = [
        'ca-long-beach-058',
        'ca-long-beach-055',
        'ca-long-beach-052',
        'ca-long-beach-049',
        'ca-long-beach-046'
    ]
    ts = msg.timestamp

    dmsg = datetime.datetime.fromtimestamp(int(round(ts / 1000, 0)), tz=datetime.timezone.utc)
    dmsg = dmsg.astimezone(datetime.timezone(datetime.timedelta(hours=-7)))

    now = datetime.datetime.now(tz=datetime.timezone.utc)
    now = now.astimezone(datetime.timezone(datetime.timedelta(hours=-7)))

    # extract the datetime from the message
    date = now.strftime('%Y-%m-%d')
    
    dt = (now - dmsg).total_seconds()
    file_exists = os.path.exists(f'./SPaT/{date}.csv')
    if msg.key in target_keys:
        print(f"Consumed message for partition {msg.partition}, key: {msg.key}, value: {msg.value}, datetime: {dmsg}, now={now}, dt={dt})")
        with open(f'./SPaT/{date}.csv', mode='a', newline='') as file:
            data = msg.value
            writer = csv.DictWriter(file, fieldnames=data.keys())

            # Write the header only if the file doesn't exist
            if file.tell() == 0:  
                writer.writeheader()
            writer.writerow(data)     

def main(args):
    key = os.environ['SASL_USERNAME']
    pw = os.environ['SASL_PASSWORD']
    topic = args.topic
    deserializer = _AvroDS(args.schema_registry, topic)

    consumer_conf = {'bootstrap_servers': args.bootstrap_servers,
                     'group_id': args.group,
                     'auto_offset_reset': "latest",
                     'sasl_plain_username': key,
                     'sasl_plain_password': pw,
                     'sasl_mechanism': 'PLAIN',
                     'security_protocol':'SASL_SSL',
                     'key_deserializer': lambda keybytes: keybytes.decode('utf-8'),
                     'value_deserializer': lambda valuebytes: deserializer.decode_value(valuebytes),
                     }

    consumer = KafkaConsumer(topic, **consumer_conf)
    try:
        for msg in consumer:
            _dump_message(msg)

    except KeyboardInterrupt:
        consumer.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="AvroDeserializer example")
    parser.add_argument('-b', dest="bootstrap_servers", required=True,
                        help="Bootstrap broker(s) (host[:port])")
    parser.add_argument('-s', dest="schema_registry", required=True,
                        help="Schema Registry (http(s)://host[:port]")
    parser.add_argument('-t', dest="topic", default="example_serde_avro",
                        help="Topic name")
    parser.add_argument('-g', dest="group", default="example_serde_avro",
                        help="Consumer group")
    main(parser.parse_args())
