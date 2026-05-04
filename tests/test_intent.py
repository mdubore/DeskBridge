from deskbridge.dm.intent import Intent, parse


def test_parse_status_keywords():
    assert parse("what's the status?") == Intent.STATUS
    assert parse("give me an update") == Intent.STATUS
    assert parse("progress report") == Intent.STATUS
    assert parse("what's happening") == Intent.STATUS


def test_parse_cancel_keywords():
    assert parse("please cancel that") == Intent.CANCEL
    assert parse("abort the run") == Intent.CANCEL
    assert parse("halt everything") == Intent.CANCEL


def test_parse_approve_keywords():
    assert parse("yes go ahead") == Intent.APPROVE
    assert parse("approve it") == Intent.APPROVE
    assert parse("confirmed, proceed") == Intent.APPROVE


def test_parse_reject_keywords():
    assert parse("no don't") == Intent.REJECT
    assert parse("reject that") == Intent.REJECT
    assert parse("deny the request") == Intent.REJECT


def test_parse_default_task():
    assert parse("fix the auth bug in service X") == Intent.TASK
    assert parse("add tests for the login flow") == Intent.TASK
    assert parse("hello") == Intent.TASK


def test_parse_status_takes_priority_over_later_rules():
    # "status update" matches STATUS rule first
    assert parse("status update please") == Intent.STATUS


def test_parse_stop_is_not_cancel_or_reject():
    # "stop" is intentionally excluded — ambiguous, safe to treat as TASK
    assert parse("stop that") == Intent.TASK


def test_parse_case_insensitive():
    assert parse("STATUS") == Intent.STATUS
    assert parse("CANCEL") == Intent.CANCEL
    assert parse("YES") == Intent.APPROVE
