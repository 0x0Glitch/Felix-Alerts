import json
import os
from pathlib import Path

class Config:
    _instance = None
    _config = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(Config, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._config is None:
            self._load_config()
    
    def _load_config(self):
        config_path = Path(__file__).parent / "config.json"
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            self._config = json.load(f)
    
    def get(self, *keys, default=None):
        """
        Get nested configuration value.
        Example: config.get('database', 'market_data_table')
        """
        value = self._config
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        return value
    
    def get_script_config(self, script_name):
        """Get all configuration for a specific script."""
        return self.get(script_name, default={})
    
    @property
    def database(self):
        return self.get('database', default={})
    
    @property
    def api(self):
        return self.get('api', default={})
    
    @property
    def symbols(self):
        return self.get('symbols', default={})

def load_config():
    """Convenience function to get config instance."""
    return Config()
