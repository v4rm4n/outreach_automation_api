# - outreach_automation_api/services/__init__.py -

from .logger import ECHO, configure_logging
from .mongo import MONGO
from .rabbit import RABBIT
from .redis import REDIS