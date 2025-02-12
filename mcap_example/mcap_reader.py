from mcap.reader import make_reader
import json

if __name__ == '__main__':
    mcap_path = "/home/nhattx/Workspace/VR/Data_platform/source/mcap_builder/output3.mcap"
    example_path = "/home/nhattx/Workspace/VR/Data_platform/source/mcap_builder/output.mcap"
    example_h1 = "/home/nhattx/Workspace/VR/Data_platform/source/mcap_builder/h1_example.mcap"
    mcap_record = "/home/nhattx/Workspace/VR/Data_platform/source/mcap_builder/demo_cam_h24_jpeg.mcap"
    with open(example_path, "rb") as f:
        reader = make_reader(f)
        with open(mcap_path, "rb") as f2:
            reader2 = make_reader(f2)


            print("a")
        # for schema, channel, message in reader.iter_messages():
        #     print(f"{channel.topic} ({schema.name})")
        #     if channel.topic == "/rgb_camera_left":
        #         print("cam")
1739258970142013440
1631270647658

