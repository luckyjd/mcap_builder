import json

COLOR = "color"
DEPTH = "depth"

with open("schema/xyz_quat.json", "r") as f:
    xyz_quat_schema_data = json.load(f)

with open("schema/compressed_image.json", "r") as f:
    compressed_image_schema_data = json.load(f)


schema_mapping = {
    "tcp": xyz_quat_schema_data,
    "tcp_base": xyz_quat_schema_data,
    # "joint" : ,
    # "gripper": ,
    # "high_freq_data": ,
    # "force_torque": ,
    # "force_torque_base": ,
}