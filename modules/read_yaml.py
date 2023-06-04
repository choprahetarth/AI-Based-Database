import yaml

class YamlBuilder():
    def __init__(self, path):
        self.path=path

    def read_yaml(self):
        with open(self.path, "rb") as stream:
            try:
                yaml_parsed = yaml.safe_load(stream)
            except yaml.YAMLError as exc:
                print(exc)
        return yaml_parsed
