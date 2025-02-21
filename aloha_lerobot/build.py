import os
import json
import time
import tyro
from datetime import datetime
from io import BytesIO
import numpy as np
from pathlib import Path
from PIL import Image
import base64
from mcap.writer import Writer

from lerobot.common.datasets.lerobot_dataset import LeRobotDataset
from aloha_lerobot.config import (
    aloha_14dof_data,
    aloha_2dof_data,
    compressed_image_schema_data,
)

list_key_2 = ["base_action"]
list_key_14 = ["observation.state", "action", "observation.velocity", "observation.effort"]
list_key_image = [
    "observation.images.cam_high",
    "observation.images.cam_left_wrist",
    "observation.images.cam_right_wrist",
    "observation.images.cam_low",
]


def mcap_builder(
        dataset_path: Path,
        output_path: Path,
        episode_idx: int = -1
):
    """
    Transform LeRobotDataset to MCAP, one episode per file.
    - If episode_idx = -1 (by default), build all episode.
    - If episode_idx >= 0 and exist, build only this episode.
    """

    if not os.path.exists(output_path):
        os.makedirs(output_path)
    else:
        assert not os.listdir(output_path), (
            "Error: Output path is not empty. Please provide an empty directory."
        )

    dataset = load_lerobot_aloha(dataset_path)

    if not len(dataset.meta.episodes):
        return "Empty Dataset"

    episodes_info = []
    start_idx_accum = 0
    for ep in dataset.meta.episodes:
        ep_idx = ep["episode_index"]
        length = ep["length"]
        episodes_info.append((ep_idx, start_idx_accum, length))
        start_idx_accum += length

    # FPS chung cho toàn bộ dataset (theo meta)
    fps = dataset.meta.fps

    for ep_idx, start_idx, length in episodes_info:
        # Nếu episode_idx != -1 và ep_idx không trùng, bỏ qua
        if episode_idx != -1 and ep_idx != episode_idx:
            continue


        episode_start_time = time.time()

        mcap_file = os.path.join(output_path, f"episode_{ep_idx}.mcap")

        ts = int(datetime.now().timestamp())

        with open(mcap_file, "wb") as f:
            writer = Writer(f)
            writer.start()

            # Đăng ký schema
            aloha_14dof_schema_id = writer.register_schema(
                name="aloha_14dof",
                encoding="jsonschema",
                data=json.dumps(aloha_14dof_data).encode("utf-8")
            )
            aloha_2dof_schema_id = writer.register_schema(
                name="aloha_2dof",
                encoding="jsonschema",
                data=json.dumps(aloha_2dof_data).encode("utf-8")
            )
            image_schema_id = writer.register_schema(
                name="foxglove.CompressedImage",
                encoding="jsonschema",
                data=json.dumps(compressed_image_schema_data).encode("utf-8")
            )

            # Duyệt tất cả frame của episode này
            for index in range(start_idx, start_idx + length):

                frame_data = dataset[index]

                for key, value in frame_data.items():
                    if key in list_key_2:
                        add_message_data(writer, key, value, ts, aloha_2dof_schema_id)
                    elif key in list_key_14:
                        add_message_data(writer, key, value, ts, aloha_14dof_schema_id)
                    elif key in list_key_image:
                        add_message_image(writer, key, value, ts, image_schema_id)

                # Cập nhật ts theo fps (mặc dù ts đang tính bằng giây,
                # nhưng increment lại là ms.
                ts += int(1000 / fps)

            writer.finish()

        # Đo thời gian kết thúc
        episode_end_time = time.time()
        episode_duration = episode_end_time - episode_start_time
        print(f"Finish Eps {ep_idx} in {episode_duration:.2f} seconds")

    return "MCAP file successfully built and stored in output path."


def compress_tensor_to_jpeg(tensor_img):
    # tensor_img: torch.Tensor [3,H,W] -> chuyển về numpy [H,W,3]
    np_img = tensor_img.numpy()  # [3,H,W]
    np_img = np.transpose(np_img, (1, 2, 0))  # [H,W,3]

    pil_img = Image.fromarray((np_img * 255).astype(np.uint8), mode='RGB')

    buffer = BytesIO()
    pil_img.save(buffer, format='JPEG')
    jpg_bytes = buffer.getvalue()

    b64_str = base64.b64encode(jpg_bytes).decode('utf-8')
    return b64_str


def add_message_data(writer, key, data, ts, schema):
    data_channel_id = writer.register_channel(
        topic=f"/data/{key}",
        message_encoding="json",
        schema_id=schema
    )

    # Nếu data là GPU tensor, đưa về CPU
    if data.is_cuda:
        data = data.cpu()

    message_data = json.dumps({
        "timestamp": {
            "sec": ts // 1000,
            "nsec": (ts % 1000) * 1_000_000.
        },
        "joint_state": data.numpy().tolist(),
    }).encode("utf-8")

    writer.add_message(
        channel_id=data_channel_id,
        log_time=ts * 1_000_000,
        publish_time=ts * 1_000_000,
        data=message_data
    )


def add_message_image(writer, key, data, ts, schema):
    data_channel_id = writer.register_channel(
        topic=f"/data/{key}",
        message_encoding="json",
        schema_id=schema
    )

    # Nếu data là GPU tensor, đưa về CPU
    if data.is_cuda:
        data = data.cpu()

    # Nén sang JPEG, encode base64
    b64_jpg = compress_tensor_to_jpeg(data)

    message_data = json.dumps({
        "timestamp": {
            "sec": ts // 1000,
            "nsec": (ts % 1000) * 1_000_000.
        },
        "frame_id": key,
        "data": b64_jpg,
        "format": "png"  # Hoặc "jpeg" tuỳ bạn
    }).encode("utf-8")

    writer.add_message(
        channel_id=data_channel_id,
        log_time=ts * 1_000_000,
        publish_time=ts * 1_000_000,
        data=message_data
    )


def load_lerobot_aloha(path):
    dataset = LeRobotDataset(repo_id="aloha", root=path, local_files_only=True)
    return dataset


if __name__ == '__main__':
    tyro.cli(mcap_builder)
