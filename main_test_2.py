import cv2
import base64
import json
import time
from datetime import datetime
from mcap.writer import Writer
from mcap.records import Schema, Channel, Message
import numpy as np
import os
import json



# Path to the video
video_path = "/home/nhattx/Workspace/VR/Data_platform/source/mcap_builder/raw_data/task_0001_user_0016_scene_0001_cfg_0003/cam_036422060909/color.mp4"
mcap_path = "output3.mcap"

# Open the video
cap = cv2.VideoCapture(video_path)
if not cap.isOpened():
    print("Error: Could not open video file.")
    exit()

# Get video properties
fps = cap.get(cv2.CAP_PROP_FPS)
print (f"FPS : {fps}")

# Define the Foxglove CompressedImage schema
compressed_image_schema = json.dumps({
    "title": "foxglove.CompressedImage",
    "description": "A compressed image",
    "$comment": "Generated by https://github.com/foxglove/schemas",
    "type": "object",
    "properties": {
        "timestamp": {
            "type": "object",
            "title": "time",
            "properties": {
                "sec": {"type": "integer", "minimum": 0},
                "nsec": {"type": "integer", "minimum": 0, "maximum": 999999999}
            },
            "description": "Timestamp of image"
        },
        "frame_id": {
            "type": "string",
            "description": "Frame of reference for the image."
        },
        "data": {
            "type": "string",
            "contentEncoding": "base64",
            "description": "Compressed image data"
        },
        "format": {
            "type": "string",
            "description": "Image format (png, jpeg, webp, etc.)"
        }
    }
}).encode("utf-8")

# Open MCAP file for writing
with open(mcap_path, "wb") as f:
    writer = Writer(f)

    # Write MCAP header
    writer.start()

    # Register schema
    schema_id = writer.register_schema(
            name="foxglove.CompressedImage",
            encoding="jsonschema",
            data=compressed_image_schema

    )

    # Register a channel
    channel_id = writer.register_channel(
            topic="/camera/036422060909/color",
            message_encoding="json",
            schema_id=schema_id

    )

    frame_count = 0
    start_time = time.time()
    cam_name = os.path.basename(video_path)
    ts_path = os.path.join(video_path.replace(cam_name, ""), "timestamps.npy")
    timestamps = np.load(ts_path, allow_pickle=True)
    ts_lst = timestamps.item()["color"]

    idx = 0
    # Read video frame-by-frame
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

        # Create a timestamp
        # print(f"date : --- {ts}")
        # dt_object = datetime.fromtimestamp(int(ts))
        # sec = int(dt_object.timestamp())
        # nsec = int(dt_object.microsecond * 1e3)  # Convert microseconds to nanoseconds
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
        timestamp = int((start_time + frame_count / fps) * 1e9)  # Convert to nanoseconds
        # timestamp = ts * 1_000_000   # tranforms to nano
        # timestamp = 1739259420627109632   # tranforms to nano
        writer.add_message(
                channel_id=channel_id,
                log_time=timestamp,
                publish_time=timestamp,
                data=message_data
        )
        idx += 1
        frame_count += 1

    cap.release()

    # Write MCAP footer
    writer.finish()

print(f"MCAP file successfully saved: {mcap_path}")
