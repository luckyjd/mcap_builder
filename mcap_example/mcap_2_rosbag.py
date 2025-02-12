from mcap.reader import make_reader
import rosbag2_py
import json
import os




if __name__ == '__main__':
    # Input MCAP file
    mcap_file = "/home/nhattx/Workspace/VR/Data_platform/source/mcap_builder/h1_example.mcap"
    # Output ROSBag 2 directory
    rosbag_dir = "converted_rosbag2"

    # Ensure the output directory exists
    # if not os.path.exists(rosbag_dir):
    #     os.makedirs(rosbag_dir)

    # Create a ROS 2 bag writer
    writer = rosbag2_py.SequentialWriter()
    storage_options = rosbag2_py.StorageOptions(uri=rosbag_dir, storage_id="sqlite3")
    converter_options = rosbag2_py.ConverterOptions(input_serialization_format="cdr", output_serialization_format="cdr")
    writer.open(storage_options, converter_options)

    with open(mcap_file, "rb") as f:
        reader = make_reader(f)

        for schema, channel, message in reader.iter_messages():
            topic_name = channel.topic
            data = message.data

            try:
                msg_json = json.loads(data.decode("utf-8"))
                serialized_msg = json.dumps(msg_json).encode("utf-8")
                writer.write(topic_name, serialized_msg, message.log_time)
            except Exception as e:
                print(f"Skipping message due to error: {e}")

    print(f"✅ MCAP file '{mcap_file}' successfully converted to ROSBag 2 at '{rosbag_dir}'")
