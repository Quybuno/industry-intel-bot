import os
from pathlib import Path
from typing import Any, Dict

import yaml
def load_yaml(path: str) -> Dict[str, Any]:
    p = Path(path)
    if not p.exists():
        return {}
    with p.open('r', encoding='utf-8') as fh:
        return yaml.safe_load(fh) or {}
class Settings:
    def __init__(self):
        self.DATABASE_URL = os.getenv('DATABASE_URL', 'sqlite:///data/dev.db')
        self.OLLAMA_BASE_URL = os.getenv('OLLAMA_BASE_URL', 'http://localhost:11434')
        self.GITHUB_TOKEN = os.getenv('GITHUB_TOKEN', '')
        self.OPENAI_API_KEY = os.getenv('OPENAI_API_KEY', '')
        self.GIT_PUBLISH_TOKEN = os.getenv('GIT_PUBLISH_TOKEN', '')
        self.SLACK_WEBHOOK_URL = os.getenv('SLACK_WEBHOOK_URL', '')
        self.LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')


def load_config_dir(path: str = 'config') -> Dict[str, Dict[str, Any]]:
    conf = {}
    p = Path(path)
    if not p.exists():
        return conf
    for f in p.glob('*.yaml'):
        conf[f.stem] = load_yaml(str(f))
    return conf


settings = Settings()
