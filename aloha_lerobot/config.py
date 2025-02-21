import json

with open("schema/14dof.json", "r") as f:
    aloha_14dof_data = json.load(f)
with open("schema/2dof.json", "r") as f:
    aloha_2dof_data = json.load(f)

with open("schema/compressed_image.json", "r") as f:
    compressed_image_schema_data = json.load(f)

