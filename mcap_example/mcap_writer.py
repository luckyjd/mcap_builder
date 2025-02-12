from mcap.writer import Writer
import time
import json

# Define the file path for the MCAP file
mcap_file = "example.mcap"

# Define a sample schema (JSON)
schema_id = 1
schema = {
    "name": "ExampleSchema",
    "encoding": "jsonschema",
    "data": b'{"type": "object", "properties": {"temperature": {"type": "number"}}}'
}

# Define a topic
topic = "sensor_data"
topic_id = 1
encoding = "json"

# Define example messages
messages = [
    {"temperature": 22.5},
    {"temperature": 23.1},
    {"temperature": 21.9},
    {"temperature": 21.9},
    {"temperature": 22.9},

]

# Open and write to the MCAP file
with open(mcap_file, "wb") as f:
    writer = Writer(f)

    # Write header
    writer.start()

    # Write schema
    schema_id = writer.register_schema(name="ExampleSchema", encoding="jsonschema", data=b'{"type": "object", "properties": {"temperature": {"type": "number"}}}')

    # Write channel
    channel_id = writer.register_channel(topic=topic, schema_id=schema_id, message_encoding=encoding)

    # Write messages
    for msg in messages:
        timestamp = int(time.time() * 1e9)  # Nanosecond precision timestamp
        writer.add_message(
            channel_id=channel_id,
            log_time=timestamp,
            publish_time=timestamp,
            data=json.dumps(msg).encode("utf-8")
        )

    # Finish writing
    writer.finish()

print(f"MCAP file '{mcap_file}' created successfully!")
