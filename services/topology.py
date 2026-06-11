# - outreach_automation_api/services/topology.py -

import yaml
from pathlib import Path
from typing import List, Literal, Dict, Any
from pydantic import BaseModel, Field
from services import ECHO

# --- Pydantic Models for Topology ---

class ExchangeConfig(BaseModel):
    name: str
    kind: str  # Allowed values: "topic", "fanout", "direct", "headers", "x-delayed-message"
    arguments: Dict[str, Any] = Field(default_factory=dict)

class QueueConfig(BaseModel):
    name: str
    queue_type: Literal["quorum", "classic"] = "classic"
    arguments: Dict[str, Any] = Field(default_factory=dict)

class BindingConfig(BaseModel):
    source: str
    destination: str
    destination_type: Literal["queue", "exchange"]
    routing_key: str = Field(default="")

class TopologyConfig(BaseModel):
    exchanges: List[ExchangeConfig]
    queues: List[QueueConfig]
    bindings: List[BindingConfig]

# --- Loading Function ---

def load_topology_config(path: str) -> TopologyConfig:
    """Loads and validates the RabbitMQ topology from a YAML file."""
    try:
        yaml_path = Path(path)
        if not yaml_path.exists():
            raise FileNotFoundError(f"Topology file not found at {path}")
        
        with open(yaml_path, 'r') as f:
            data = yaml.safe_load(f)
            
        config = TopologyConfig.model_validate(data)
        ECHO.debug(f"[cyan]topology:[/] Loaded and validated configuration from {path}")
        return config
    except Exception as e:
        ECHO.error(f"[cyan]topology:[/] Failed to load or parse topology YAML: {e}")
        raise