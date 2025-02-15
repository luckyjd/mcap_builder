import os
import numpy as np
import mcap
from mcap.writer import Writer
from mcap_ros2.writer import Ros2Writer
from geometry_msgs.msg import PoseStamped
from builtin_interfaces.msg import Time

# Định nghĩa đường dẫn file
INPUT_FILE = "RH20T/task_0001_user_0001_scene_0001_cfg_0001/transformed/tcp_base.npy"
OUTPUT_MCAP = "output.mcap"

# Đọc dữ liệu từ tcp_base.npy
tcp_data = np.load(INPUT_FILE, allow_pickle=True).item()

# Khởi tạo writer cho MCAP
writer = Writer(open(OUTPUT_MCAP, "wb"))

def write_tcp_data(serial_number, tcp_entries):
    """Ghi dữ liệu TCP vào file MCAP"""
    for entry in tcp_entries:
        timestamp = entry["timestamp"]
        tcp_pose = entry["tcp"]  # xyz + quaternion (7D)

        # Chuyển timestamp thành ROS2 Time
        ros_time = Time(sec=int(timestamp), nanosec=int((timestamp % 1) * 1e9))

        # Tạo PoseStamped message
        msg = PoseStamped()
        msg.header.stamp = ros_time
        msg.header.frame_id = "base_link"
        msg.pose.position.x, msg.pose.position.y, msg.pose.position.z = tcp_pose[:3]
        msg.pose.orientation.x, msg.pose.orientation.y, msg.pose.orientation.z, msg.pose.orientation.w = tcp_pose[3:]

        # Ghi vào MCAP (Mỗi serial_number sẽ có topic riêng)
        topic = f"/robot/{serial_number}/tcp_base/pose"
        writer.write_message(topic, msg, log_time=int(timestamp))

# Ghi tất cả các serial_number
for serial_number, tcp_entries in tcp_data.items():
    write_tcp_data(serial_number, tcp_entries)

# Kết thúc MCAP
writer.finish()
print(f"✅ MCAP file saved: {OUTPUT_MCAP}")
