import json
import logging


class Config:
    def __init__(self, path: str):
        try:
            with open(path) as file:
                self._config = json.loads(file.read())
        except TypeError:
            logging.error('Specify config-file (--config <path>)')
            exit(1)
        except FileNotFoundError:
            logging.error(f'Config-file {path} is not found')
            exit(1)

    def __getitem__(self, item: str):
        return self._config[item]
