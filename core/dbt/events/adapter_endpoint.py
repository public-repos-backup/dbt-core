from dataclasses import dataclass
from dbt.events.functions import fire_event
from dbt.events.types import (
    AdapterEventDebug, AdapterEventInfo, AdapterEventWarning, AdapterEventError
)


@dataclass
class AdapterLogger():
    name: str

    def debug(self, msg: str):
        fire_event(AdapterEventDebug(name=self.name, raw_msg=msg))

    def info(self, msg: str):
        fire_event(AdapterEventInfo(name=self.name, raw_msg=msg))

    def warning(self, msg: str):
        fire_event(AdapterEventWarning(name=self.name, raw_msg=msg))

    def error(self, msg: str):
        fire_event(AdapterEventError(name=self.name, raw_msg=msg))
