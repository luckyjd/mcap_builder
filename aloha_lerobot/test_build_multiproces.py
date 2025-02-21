import os
import json
import tyro
import base64
import time
from datetime import datetime
from io import BytesIO
import numpy as np
from pathlib import Path
from PIL import Image
from multiprocessing import Pool, cpu_count

from lerobot.common.datasets.lerobot_dataset import LeRobotDataset
from mcap.writer import Writer
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


def mcap_builder(dataset_path: Path, output_path: Path):
    """
    Chuyển dữ liệu từ LeRobotDataset sang định dạng MCAP,
    mỗi episode một file .mcap.

    - Sử dụng multiprocessing để nén ảnh song song.
    - Tránh gọi .numpy() nhiều lần trên GPU.
    - Vẫn base64 cho ảnh, vẫn ghi tuần tự (mỗi tập tin MCAP cho 1 episode).
    - Tính và in ra thời gian xử lý của mỗi episode.
    """

    if not os.path.exists(output_path):
        os.makedirs(output_path)
    else:
        assert not os.listdir(output_path), (
            "Error: Output path is not empty. "
            "Please provide an empty directory."
        )

    dataset = load_lerobot_aloha(dataset_path)
    if not len(dataset.meta.episodes):
        return "Empty Dataset"

    start_idx = 0

    # Thiết lập multiprocessing để nén ảnh
    num_processes = max(1, cpu_count() - 1)
    pool = Pool(processes=num_processes)

    for episode in dataset.meta.episodes:
        # Bắt đầu tính thời gian cho episode này
        episode_start_time = time.time()

        length = episode["length"]
        fps = dataset.meta.fps

        mcap_file = os.path.join(
            output_path,
            f"episode_{episode['episode_index']}.mcap",
        )
        ts = int(datetime.now().timestamp() * 1000)  # ms

        # Gom toàn bộ frame data cho episode này
        episode_data = [dataset[i] for i in range(start_idx, start_idx + length)]

        # Tách joint_entries và image_entries
        joint_entries = []
        image_entries = []

        for frame_data in episode_data:
            for key, value in frame_data.items():
                if key in list_key_2 or key in list_key_14:
                    joint_entries.append((key, value))
                elif key in list_key_image:
                    image_entries.append((key, value))

        # Nén ảnh song song
        # map() trả về list kết quả tương ứng thứ tự
        compressed_results = pool.map(compress_image_worker, image_entries)

        # Ghi MCAP (tuần tự)
        with open(mcap_file, "wb") as f:
            writer = Writer(f)
            writer.start()

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

            compressed_iter = iter(compressed_results)

            # Duyệt lại từng frame để ghi
            for frame_data in episode_data:
                for key, value in frame_data.items():
                    if key in list_key_2:
                        add_message_data(
                            writer, key, value, ts, aloha_2dof_schema_id
                        )
                    elif key in list_key_14:
                        add_message_data(
                            writer, key, value, ts, aloha_14dof_schema_id
                        )
                    elif key in list_key_image:
                        # Lấy kết quả ảnh nén
                        img_key, base64_jpg = next(compressed_iter)
                        add_message_image(
                            writer, img_key, base64_jpg, ts, image_schema_id
                        )

                # Tăng timestamp để phù hợp fps
                ts += int(1000 / fps)

            writer.finish()

        # Tính thời gian đã xử lý xong 1 episode
        episode_end_time = time.time()
        episode_duration = episode_end_time - episode_start_time
        print(
            f"Finish Eps {episode['episode_index']} in {episode_duration:.2f} seconds"
        )

        start_idx += length

    # Đóng pool
    pool.close()
    pool.join()

    return "MCAP file successfully built and stored in output path."


def compress_image_worker(args):
    """
    Worker nén ảnh:
    - Nhận (key, tensor_img).
    - Nếu đang ở GPU => chuyển CPU.
    - Gọi .numpy() đúng 1 lần, nén PIL -> JPEG, trả về base64.
    """
    key, tensor_img = args
    if tensor_img.is_cuda:
        tensor_img = tensor_img.cpu()

    np_img = tensor_img.numpy()  # [3,H,W]
    np_img = np.transpose(np_img, (1, 2, 0))  # [H,W,3]

    pil_img = Image.fromarray((np_img * 255).astype(np.uint8), mode="RGB")
    buffer = BytesIO()
    pil_img.save(buffer, format="JPEG")
    jpg_bytes = buffer.getvalue()

    b64_str = base64.b64encode(jpg_bytes).decode("utf-8")
    return key, b64_str


def add_message_data(writer, key, data, ts, schema_id):
    if data.is_cuda:
        data = data.cpu()
    joint_list = data.numpy().tolist()

    data_channel_id = writer.register_channel(
        topic=f"/data/{key}", message_encoding="json", schema_id=schema_id
    )

    message_data = json.dumps({
        "timestamp": {
            "sec": ts // 1000,
            "nsec": (ts % 1000) * 1_000_000
        },
        "joint_state": joint_list,
    }).encode("utf-8")

    writer.add_message(
        channel_id=data_channel_id,
        log_time=ts * 1_000_000,
        publish_time=ts * 1_000_000,
        data=message_data
    )


def add_message_image(writer, key, base64_jpg, ts, schema_id):
    data_channel_id = writer.register_channel(
        topic=f"/data/{key}", message_encoding="json", schema_id=schema_id
    )
    message_data = json.dumps({
        "timestamp": {
            "sec": ts // 1000,
            "nsec": (ts % 1000) * 1_000_000
        },
        "frame_id": key,
        "data": base64_jpg,
        "format": "jpeg"
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
