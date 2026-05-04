import enum
import re


class Intent(enum.Enum):
    TASK = "task"
    STATUS = "status"
    CANCEL = "cancel"
    APPROVE = "approve"
    REJECT = "reject"


_RULES: list[tuple[Intent, re.Pattern]] = [
    (Intent.STATUS,  re.compile(r"\b(status|update|progress|what.?s happening)\b", re.I)),
    (Intent.CANCEL,  re.compile(r"\b(cancel|abort|halt)\b", re.I)),
    (Intent.APPROVE, re.compile(r"\b(approve|yes|go ahead|confirmed|proceed)\b", re.I)),
    (Intent.REJECT,  re.compile(r"\b(reject|no|deny|don.?t)\b", re.I)),
]


def parse(text: str) -> Intent:
    for intent, pattern in _RULES:
        if pattern.search(text):
            return intent
    return Intent.TASK
