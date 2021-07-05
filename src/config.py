import json


class Config:
    def __init__(self, path):
        with open(path) as file:
            self._config = json.loads(file.read())

    def __getitem__(self, item):
        return self._config[item]
