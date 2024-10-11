import argparse
import csv
from confluent_kafka import Consumer
import json
import time
import avro.schema
import avro.io
import io
import os
import pandas as pd

# Avro schema as a Python dictionary
schema_json = {
    "type": "record",
    "namespace": "calc.ntcip",
    "name": "gammas",
    "fields": [
        {"name": "xid", "type": ["null", "string"], "default": None, "doc": "Unique intersection ID"},
        {"name": "localZeroTime", "type": ["null", "string"], "default": None, "doc": "Time of coordinated cycle start on controller"},
        {"name": "phaseStart", "type": ["null", "string"], "default": None, "doc": "Start of phase green for this phaseId"},
        {"name": "phaseId", "type": ["null", "int"], "default": None, "doc": "The number of the NTCIP phase"},
        {"name": "normGamma", "type": ["null", "int"], "default": None, "doc": "Final observed gamma value, normalized"},
        {"name": "delta", "type": ["null", "int"], "default": None, "doc": "Difference between final observed gamma and expected ideal value"},
        {"name": "localFreeStatus", "type": ["null", "string"], "default": None, "doc": "NTCIP 1202 variable indicating coordinated, free, or other mode of operation"},
        {"name": "rawGamma", "type": ["null", "float"], "default": None, "doc": "Raw gamma input value from edge"},
        {"name": "idealGammma", "type": ["null", "float"], "default": None, "doc": "Expected gamma given the sequence and splits of a running pattern if the controller runs the exact splits"},
        {"name": "coordSyncPointMethod", "type": ["null", "string"], "default": None, "doc": "startOfGreen or startOfYellow, the phase state of the main coord phase at local zero point"},
        {"name": "patternNumber", "type": ["null", "int"], "default": None},
        {"name": "patternCycle", "type": ["null", "int"], "default": None},
        {"name": "patternOffset", "type": ["null", "int"], "default": None},
        {"name": "patternSplitNumber", "type": ["null", "int"], "default": None},
        {"name": "patternSequenceNumber", "type": ["null", "int"], "default": None}
    ]
}

# Replace with your actual configuration details
conf = {
    'bootstrap.servers': 'pkc-rgm37.us-west-2.aws.confluent.cloud:9092', 
    'security.protocol': 'SASL_SSL',
    'sasl.mechanisms': 'PLAIN',
    'sasl.username': '', # replace with your actual key
    'sasl.password': '', # replace with your actual secret
    'group.id': 'ucrcycles',  # Consumer group ID
    # 'auto.offset.reset': 'latest'
}

# Convert the Python dictionary to a JSON string
schema_str = json.dumps(schema_json)
schema = avro.schema.parse(str(schema_str))

consumer = Consumer(conf)

# Subscribe to the Kafka topic
topic = 'calc.ntcip.gammas'
consumer.subscribe([topic])

# Function to decode Kafka messages with Avro schema
def decode_avro_message(schema, message):
    bytes_reader = io.BytesIO(message[5:])
    decoder = avro.io.BinaryDecoder(bytes_reader)
    reader = avro.io.DatumReader(schema)
    return reader.read(decoder)

# Save message to CSV
def save_to_csv(output_folder, key, data):
    if os.path.exists(output_folder) == False:
        os.makedirs(output_folder)

    filename = os.path.join(output_folder,f"{key}.csv")
    
    with open(filename, 'a', newline='') as csvfile:
        # add a column for the timestamp
        data['receive_timestamp'] = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime())
        fieldnames = data.keys()
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        
        if csvfile.tell() == 0:  # Write header if the file is empty
            writer.writeheader()
        writer.writerow(data)

def process_csv(output_folder, key):
    # convert the localZeroTime,phaseStart from UTC to PDT time zone
    filename = os.path.join(output_folder,f"{key}.csv")
    df = pd.read_csv(filename)
    df['localZeroTime'] = pd.to_datetime(df['localZeroTime'], utc=True)
    df['phaseStart'] = pd.to_datetime(df['phaseStart'], utc=True)
    df['localZeroTime'] = df['localZeroTime'].dt.tz_convert('US/Pacific')
    df['phaseStart'] = df['phaseStart'].dt.tz_convert('US/Pacific')
    df['receive_timestamp'] = pd.to_datetime(df['receive_timestamp']).dt.tz_localize('US/Pacific')
    # calculate the time difference between phaseStart and receive_timestamp
    df['time_diff'] = (df['receive_timestamp'] - df['phaseStart']).dt.total_seconds()
    # add prefix to the filename
    df_filename = os.path.join(output_folder,f"processed_{key}.csv")
    df.to_csv(df_filename, index=False)


def main(args):
    desired_key = args.key
    output_folder = args.output_folder
    print(f"Listening for messages with key: {desired_key}")

    try:
        while True:
            msg = consumer.poll()  # Poll for new messages
            if msg is not None and msg.error() is None:
                key = msg.key().decode('utf-8')
                if key != desired_key:
                    print("No new messages for the desired key")
                    print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
                    print('-------------------')
                    continue
                
                # Decode the message
                decoded_value = decode_avro_message(schema, msg.value())
                
                print(f"Consumed message for key: {key}, value: {decoded_value}")
                print('-------------------')
                # Save to CSV for the specific key
                save_to_csv(output_folder, key, decoded_value)

            else:
                print("No new messages")
                print(time.strftime('%Y-%m-%d %H:%M:%S', time.localtime()))
                
    except KeyboardInterrupt:
        print("Consumer interrupted by user.")

    finally:
        consumer.close()
        process_csv(output_folder, desired_key)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Subscribe to Kafka topic and save specific key messages to CSV.')
    parser.add_argument('--key', required=True, help="The target key to filter messages by.")
    parser.add_argument('--output_folder', required=False, help="The output CSV file to save the messages to.")
    args = parser.parse_args()
    main(args)
