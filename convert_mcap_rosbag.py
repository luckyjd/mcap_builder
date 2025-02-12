import os
from mcap.reader import McapReader
from rosbag2_py import SequentialWriter
from rosbag2_py._storage import StorageOptions, ConverterOptions

# Define input MCAP file and output ROS 2 bag directory
mcap_file = "h1_example.mcap"
output_bag = "output_bag"

# Create ROS 2 bag writer
writer = SequentialWriter()
storage_options = StorageOptions(uri=output_bag, storage_id="sqlite3")
converter_options = ConverterOptions(input_serialization_format="cdr", output_serialization_format="cdr")
writer.open(storage_options, converter_options)

# Read MCAP file and write to ROS 2 bag
with open(mcap_file, "rb") as f:
    reader = McapReader(f)
    for message in reader.iter_messages():
        writer.write(message.topic, message.message, message.log_time)

print(f"Conversion complete: {mcap_file} → {output_bag}")
