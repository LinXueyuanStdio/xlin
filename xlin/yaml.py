import yaml


def load_yaml(yaml_file: str):
    with open(yaml_file, "r", encoding="utf-8") as f:
        file_str = f.read()
    schema = yaml.safe_load(file_str)
    return schema
