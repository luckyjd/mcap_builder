import os
import json
import glob
import cv2
import base64
import numpy as np
from mcap.writer import Writer
from rh20t.config import COLOR, DEPTH, compressed_image_schema_data, xyz_quat_schema_data, schema_mapping


def mcap_builder(output_mcap, scene_path):
    with open(output_mcap, "wb") as f:
        writer = Writer(f)
        writer.start()
        schema = {}

        xyz_quat_schema_id = writer.register_schema(
            name="GripperPose",
            encoding="jsonschema",
            data=json.dumps(xyz_quat_schema_data).encode("utf-8")
        )

        transformed_files = glob.glob(f"{os.path.join(scene_path, 'transformed')}/*.npy")
        for transformed_file in transformed_files:
            transform_data(writer, transformed_file, xyz_quat_schema_id)
        image_schema_id = writer.register_schema(
            name="foxglove.CompressedImage",
            encoding="jsonschema",
            data=json.dumps(compressed_image_schema_data).encode("utf-8")
        )
        camera_dirs = glob.glob(os.path.join(scene_path, "cam_*"))

        for cam_dir in camera_dirs:
            ts_dict = load_camera_timestamps(cam_dir)
            add_color_frames_from_cam(writer=writer, cam_folder=cam_dir, timestamps=ts_dict, image_schema_id=image_schema_id)
            add_depth_frames_from_cam(writer=writer, cam_folder=cam_dir, timestamps=ts_dict, image_schema_id=image_schema_id)
        writer.finish()

def transform_data(writer, file_path, schema_id):
    if "tcp_base" in file_path:
        if not file_path.endswith(".npy"):
            return
        file_name = os.path.basename(file_path).replace(".npy", "")
        data = np.load(file_path, allow_pickle=True).item()
        for cam_serial_number, entries in data.items():
            data_channel_id = writer.register_channel(
                topic=f"/data/{cam_serial_number}/{file_name}",
                message_encoding="json",
                schema_id=schema_id
            )
            for entry in entries:
                timestamp = entry["timestamp"]
                tcp_pose = entry["tcp"]
                data = {
                    "timestamp": timestamp,
                    "position": {
                        "x": tcp_pose[0],
                        "y": tcp_pose[1],
                        "z": tcp_pose[2]
                    },
                    "orientation": {
                        "x": tcp_pose[3],
                        "y": tcp_pose[4],
                        "z": tcp_pose[5],
                        "w": tcp_pose[6]
                    }
                }
                writer.add_message(
                    channel_id=data_channel_id,
                    log_time=timestamp* 1_000_000,
                    publish_time=timestamp* 1_000_000,
                    data=json.dumps(data).encode("utf-8")
                )


def load_camera_timestamps(cam_folder):
    ts_path = os.path.join(cam_folder, "timestamps.npy")
    timestamps = np.load(ts_path, allow_pickle=True)
    return timestamps.item()


def add_color_frames_from_cam(writer, cam_folder, timestamps, image_schema_id):
    cam_path = os.path.join(cam_folder, f"{COLOR}.mp4")
    if not os.path.exists(cam_path):
        return
    cam_name = os.path.basename(cam_folder)  # Get only "cam_*"
    cam_number = cam_name.replace("cam_", "")
    color_channel_id = writer.register_channel(
        topic=f"/camera/{cam_number}/{COLOR}",
        message_encoding="json",
        schema_id=image_schema_id
    )
    ts_lst = timestamps[COLOR]
    cap = cv2.VideoCapture(cam_path)
    # fps = cap.get(cv2.CAP_PROP_FPS)
    idx = 0
    # frame_count = 0
    while True:
        ret, frame = cap.read()
        if not ret:
            break  # Stop if video ends
        if not ret or idx >= len(ts_lst):
            break
        ts = ts_lst[idx]
        # Encode frame as PNG
        success, buffer = cv2.imencode(".png", frame)
        if not success:
            continue

        # Convert image to base64
        base64_data = base64.b64encode(buffer).decode("utf-8")

        ts = int(ts)

        sec = ts // 1000
        nsec = (ts % 1000) * 1_000_000

        # Prepare the JSON message
        message_data = json.dumps({
            "timestamp": {"sec": sec, "nsec": nsec},
            "frame_id": "camera_1",
            "data": base64_data,
            "format": "png"
        }).encode("utf-8")

        # Write message to MCAP
        # timestamp = int((start_time + frame_count / fps) * 1e9)
        # timestamp = int((ts_lst[0] + frame_count / fps) * 1_000_000)  # Convert to nanoseconds
        timestamp = ts * 1_000_000  # tranforms to nano
        # timestamp = 1739259420627109632   # tranforms to nano
        writer.add_message(
            channel_id=color_channel_id,
            log_time=timestamp,
            publish_time=timestamp,
            data=message_data
        )
        idx += 1
        # frame_count += 1
    cap.release()

def add_depth_frames_from_cam(writer, cam_folder, timestamps, image_schema_id, size=(640, 360)):
    cam_path = os.path.join(cam_folder, f"{DEPTH}.mp4")
    if not os.path.exists(cam_path):
        return
    cam_name = os.path.basename(cam_folder)  # Get only "cam_*"
    cam_number = cam_name.replace("cam_", "")
    color_channel_id = writer.register_channel(
        topic=f"/camera/{cam_number}/{DEPTH}",
        message_encoding="json",
        schema_id=image_schema_id
    )
    ts_lst = timestamps[DEPTH]
    width, height = size
    cap = cv2.VideoCapture(cam_path)
    # fps = cap.get(cv2.CAP_PROP_FPS)
    is_l515 = ("cam_f" in cam_path)
    idx = 0
    # frame_count = 0
    while True:

        ret, frame = cap.read()
        if not ret or idx >= len(ts_lst):
            break
        ts = ts_lst[idx]
        gray = cv2.cvtColor(frame, cv2.COLOR_BGR2GRAY)
        gray1 = np.array(gray[:height, :]).astype(np.int32)
        gray2 = np.array(gray[height:, :]).astype(np.int32)
        gray = np.array(gray2 * 256 + gray1).astype(np.uint16)
        if is_l515:
            gray = gray * 4
        # cv2.imwrite(os.path.join("/home/nhattx/Workspace/VR/Data_platform/source/mcap_builder_git/mcap_example", '{}.png'.format(ts)), gray)
        success, buffer = cv2.imencode(".png", gray)
        if not success:
            continue

        # Convert image to base64
        base64_data = base64.b64encode(buffer).decode("utf-8")

        ts = int(ts)

        sec = ts // 1000
        nsec = (ts % 1000) * 1_000_000

        # Prepare the JSON message
        message_data = json.dumps({
            "timestamp": {"sec": sec, "nsec": nsec},
            "frame_id": f"cam_{cam_number}",
            "data": base64_data,
            "format": "png"
        }).encode("utf-8")

        # Write message to MCAP
        # timestamp = int((start_time + frame_count / fps) * 1e9)
        # timestamp = int((ts_lst[0] + frame_count / fps) * 1_000_000)  # Convert to nanoseconds
        timestamp = ts * 1_000_000  # tranforms to nano
        # timestamp = 1739259420627109632   # tranforms to nano
        writer.add_message(
            channel_id=color_channel_id,
            log_time=timestamp,
            publish_time=timestamp,
            data=message_data
        )
        idx += 1
        # frame_count += 1
    cap.release()


if __name__ == '__main__':
    output_mcap = "output2.mcap"
    # scene_path = "raw_data/task_0004_user_0016_scene_0003_cfg_0003"
    scene_path = "raw_data/task_0001_user_0016_scene_0001_cfg_0003"
    mcap_builder(output_mcap, scene_path)
