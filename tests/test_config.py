import textwrap
import pytest
from deskbridge.config import DeskBridgeConfig, load_config, ConfigError


MINIMAL_CONFIG = textwrap.dedent("""
    [supervisor]
    db_path = "/tmp/test.db"

    [mcp]
    command = "nostrdesk-mcp"

    [[identities]]
    label = "alice"
    npub = "npub1alice"
    passphrase_ref = "env:ALICE_PASSPHRASE"

    [[projects]]
    id = "proj-1"
    name = "Test Project"
    repo_path = "/tmp/repo"
    identity = "alice"
    escalation_dm_target = "npub1human"
""")


def test_load_minimal_config(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)
    config = load_config(cfg_file)
    assert isinstance(config, DeskBridgeConfig)
    assert config.supervisor.db_path == "/tmp/test.db"
    assert len(config.identities) == 1
    assert config.identities[0].label == "alice"
    assert len(config.projects) == 1
    assert config.projects[0].id == "proj-1"


def test_default_supervisor_values(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)
    config = load_config(cfg_file)
    assert config.supervisor.log_level == "INFO"
    assert config.supervisor.heartbeat_interval_secs == 60.0
    assert config.mcp.startup_timeout_secs == 30


def test_project_references_unknown_identity_raises(tmp_path):
    bad = MINIMAL_CONFIG.replace('identity = "alice"', 'identity = "nobody"')
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(bad)
    with pytest.raises(ConfigError, match="unknown identity"):
        load_config(cfg_file)


def test_missing_required_field_raises(tmp_path):
    bad = textwrap.dedent("""
        [supervisor]
        db_path = "/tmp/test.db"
        [mcp]
        command = "nostrdesk-mcp"
    """)
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(bad)
    with pytest.raises(ConfigError, match="identities"):
        load_config(cfg_file)


def test_passphrase_ref_env_resolved(tmp_path, monkeypatch):
    monkeypatch.setenv("ALICE_PASSPHRASE", "s3cr3t")
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)
    config = load_config(cfg_file)
    passphrase = config.identities[0].resolve_passphrase()
    assert passphrase == "s3cr3t"


def test_passphrase_ref_env_missing_raises(tmp_path, monkeypatch):
    monkeypatch.delenv("ALICE_PASSPHRASE", raising=False)
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)
    config = load_config(cfg_file)
    with pytest.raises(ConfigError, match="ALICE_PASSPHRASE"):
        config.identities[0].resolve_passphrase()


def test_passphrase_ref_invalid_format_raises():
    from deskbridge.config import IdentityConfig
    identity = IdentityConfig(
        label="alice", npub="npub1alice", passphrase_ref="plaintext_secret"
    )
    with pytest.raises(ConfigError, match="Unknown passphrase_ref format"):
        identity.resolve_passphrase()


def test_passphrase_ref_keyring_bad_format_raises():
    from deskbridge.config import IdentityConfig
    identity = IdentityConfig(
        label="alice", npub="npub1alice", passphrase_ref="keyring:onlyone"
    )
    with pytest.raises(ConfigError, match="expected keyring:service:key"):
        identity.resolve_passphrase()


def test_empty_identities_raises(tmp_path):
    from deskbridge.config import SupervisorConfig, McpConfig
    with pytest.raises(ValueError, match="at least one"):
        DeskBridgeConfig(
            supervisor=SupervisorConfig(db_path="/tmp/test.db"),
            mcp=McpConfig(command="nostrdesk-mcp"),
            identities=[]
        )


def test_identity_config_operator_npub_defaults_to_none():
    from deskbridge.config import IdentityConfig
    identity = IdentityConfig(label="alice", npub="npub1alice", passphrase_ref="env:X")
    assert identity.operator_npub is None


def test_identity_config_operator_npub_can_be_set():
    from deskbridge.config import IdentityConfig
    identity = IdentityConfig(
        label="alice", npub="npub1alice", passphrase_ref="env:X",
        operator_npub="npub1op"
    )
    assert identity.operator_npub == "npub1op"


def test_project_config_boards_defaults_to_empty():
    from deskbridge.config import ProjectConfig
    proj = ProjectConfig(
        id="p1", name="N", repo_path="/r",
        identity="alice", escalation_dm_target="npub1op"
    )
    assert proj.boards == []


def test_project_config_kanban_columns_default():
    from deskbridge.config import ProjectConfig
    proj = ProjectConfig(
        id="p1", name="N", repo_path="/r",
        identity="alice", escalation_dm_target="npub1op"
    )
    assert proj.kanban_column_in_progress == "in_progress"
    assert proj.kanban_column_done == "done"


def test_project_config_kanban_fields_parse_from_toml(tmp_path):
    content = textwrap.dedent("""
        [supervisor]
        db_path = "/tmp/test.db"

        [mcp]
        command = "nostrdesk-mcp"

        [[identities]]
        label = "alice"
        npub = "npub1alice"
        passphrase_ref = "env:ALICE_PASSPHRASE"

        [[projects]]
        id = "proj-1"
        name = "Test Project"
        repo_path = "/tmp/repo"
        identity = "alice"
        escalation_dm_target = "npub1human"
        boards = ["ch-abc", "ch-def"]
        kanban_column_in_progress = "doing"
        kanban_column_done = "finished"
    """)
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(content)
    config = load_config(cfg_file)
    proj = config.projects[0]
    assert proj.boards == ["ch-abc", "ch-def"]
    assert proj.kanban_column_in_progress == "doing"
    assert proj.kanban_column_done == "finished"
