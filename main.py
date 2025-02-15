from rh20t import mcap_builder

if __name__ == '__main__':
    output_mcap = "output2.mcap"
    # scene_path = "raw_data/task_0004_user_0016_scene_0003_cfg_0003"
    scene_path = "raw_data/task_0001_user_0016_scene_0001_cfg_0003"
    mcap_builder(output_mcap, scene_path)