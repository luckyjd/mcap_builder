# MCAP creator tool


## Install env 
- Install UV : https://docs.astral.sh/uv/getting-started/installation/
  - `$ pip install uv`

- Setup UV :
  - `$ cd /project/path/where/include/uv.lock`
  - `$ uv sync`
  - `$ source .env/bin/activate`

- Each dataset folder have it's own `requirements.txt` . You can install only this file for only this dataset

## Run convert script
- Example /aloha_lerobot --> mcap :
  - `$ cd /project/path/`
  - `$ export PYTHONPATH=/project/path`
  - `$ python aloha_lerobot/build.py --help`
```
usage: build.py [-h] --dataset-path PATH --output-path PATH [--episode-idx INT]

Transform LeRobotDataset to MCAP, one episode per file.
- If episode_idx = -1 (by default), build all episode.
- If episode_idx >= 0 and exist, build only this episode.

╭─ options ───────────────────────────────────────────────╮
│ -h, --help              show this help message and exit │
│ --dataset-path PATH     (required)                      │
│ --output-path PATH      (required)                      │
│ --episode-idx INT       (default: -1)                   │
╰─────────────────────────────────────────────────────────╯

``` 
  - `$ python aloha_lerobot/build.py --dataset-path=/home/nhattx/Workspace/VR/Study_robotics/dataset/LEROBOT/aloha_mobile_cabinet --output-path=output_aloha`
  - wait for transform data


## Install foxglove for visualize

- Create foxglove account
- Download foxglove : https://foxglove.dev/download
- Install, login
- Open app > Open local file > point to mcap file
- Add layout ( or download from  [Here](https://vingroupjsc-my.sharepoint.com/:u:/g/personal/vr_project01_vingroup_net/EQPjUiuQjqBLjUuADBPV8nMBuhlbyg0HBJ9osjh7AWzUFg?e=JJLx0d))
 


