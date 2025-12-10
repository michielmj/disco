from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class EventEnvelopeMsg(_message.Message):
    __slots__ = ["target_node", "target_simproc", "epoch", "data", "headers"]
    class HeadersEntry(_message.Message):
        __slots__ = ["key", "value"]
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    TARGET_NODE_FIELD_NUMBER: _ClassVar[int]
    TARGET_SIMPROC_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    HEADERS_FIELD_NUMBER: _ClassVar[int]
    target_node: str
    target_simproc: str
    epoch: float
    data: bytes
    headers: _containers.ScalarMap[str, str]
    def __init__(self, target_node: _Optional[str] = ..., target_simproc: _Optional[str] = ..., epoch: _Optional[float] = ..., data: _Optional[bytes] = ..., headers: _Optional[_Mapping[str, str]] = ...) -> None: ...

class PromiseEnvelopeMsg(_message.Message):
    __slots__ = ["target_node", "target_simproc", "seqnr", "epoch", "num_events"]
    TARGET_NODE_FIELD_NUMBER: _ClassVar[int]
    TARGET_SIMPROC_FIELD_NUMBER: _ClassVar[int]
    SEQNR_FIELD_NUMBER: _ClassVar[int]
    EPOCH_FIELD_NUMBER: _ClassVar[int]
    NUM_EVENTS_FIELD_NUMBER: _ClassVar[int]
    target_node: str
    target_simproc: str
    seqnr: int
    epoch: float
    num_events: int
    def __init__(self, target_node: _Optional[str] = ..., target_simproc: _Optional[str] = ..., seqnr: _Optional[int] = ..., epoch: _Optional[float] = ..., num_events: _Optional[int] = ...) -> None: ...

class TransportAck(_message.Message):
    __slots__ = ["message"]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    message: str
    def __init__(self, message: _Optional[str] = ...) -> None: ...
