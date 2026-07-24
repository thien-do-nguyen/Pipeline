from dataclasses import dataclass
from enum import StrEnum


class ChangeOperation(StrEnum):
    INSERT = "INSERT"
    UPDATE = "UPDATE"
    DELETE = "DELETE"


@dataclass(frozen=True)
class EventCursor:
    last_event_id: int

    def __post_init__(self) -> None:
        if self.last_event_id < 0:
            raise ValueError("last_event_id must be non-negative")
