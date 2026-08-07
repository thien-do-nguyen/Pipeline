from enum import StrEnum

CDC_SCHEMA_VERSION = 1


class DebeziumOperation(StrEnum):
    SNAPSHOT = "r"
    CREATE = "c"
    UPDATE = "u"
    DELETE = "d"


VALID_DEBEZIUM_OPERATIONS = tuple(operation.value for operation in DebeziumOperation)
