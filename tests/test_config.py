import textwrap
import pytest
from deskbridge.config import DeskBridgeConfig, load_config, ConfigError
from deskbridge.agent.adapters import KNOWN_ADAPTERS


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


def test_identity_config_passphrase_env_var_returns_env_name():
    from deskbridge.config import IdentityConfig
    identity = IdentityConfig(
        label="alice", npub="npub1alice", passphrase_ref="env:ALICE_PASSPHRASE"
    )
    assert identity.passphrase_env_var() == "ALICE_PASSPHRASE"


def test_identity_config_passphrase_env_var_ignores_keyring_refs():
    from deskbridge.config import IdentityConfig
    identity = IdentityConfig(
        label="alice",
        npub="npub1alice",
        passphrase_ref="keyring:nostrdesk:alice",
    )
    assert identity.passphrase_env_var() is None


def test_identity_config_passphrase_env_var_ignores_empty_env_ref():
    from deskbridge.config import IdentityConfig
    identity = IdentityConfig(label="alice", npub="npub1alice", passphrase_ref="env:")
    assert identity.passphrase_env_var() is None


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


def test_project_config_check_in_interval_hours_accepted(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG.rstrip() + "\ncheck_in_interval_hours = 24.0\n")
    config = load_config(cfg_file)
    assert config.projects[0].check_in_interval_hours == 24.0


def test_project_config_check_in_fields_absent_by_default(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)
    config = load_config(cfg_file)
    assert config.projects[0].check_in_interval_hours is None
    assert config.projects[0].check_in_prompt == (
        "Perform a project status check-in and report any blockers or progress."
    )


def test_project_config_check_in_interval_zero_rejected(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG.rstrip() + "\ncheck_in_interval_hours = 0.0\n")
    with pytest.raises(ConfigError):
        load_config(cfg_file)


def test_project_config_check_in_interval_negative_rejected(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG.rstrip() + "\ncheck_in_interval_hours = -1.0\n")
    with pytest.raises(ConfigError):
        load_config(cfg_file)


def test_project_config_check_in_prompt_can_be_overridden(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(
        MINIMAL_CONFIG.rstrip()
        + '\ncheck_in_prompt = "Weekly sync: any blockers?"\n'
    )
    config = load_config(cfg_file)
    assert config.projects[0].check_in_prompt == "Weekly sync: any blockers?"


def test_adapter_defaults_to_claude_code():
    from deskbridge.config import ProjectConfig
    proj = ProjectConfig(
        id="p1", name="MyProj", repo_path="/repo",
        identity="alice", escalation_dm_target="npub1op",
    )
    assert proj.adapter == "claude-code"


@pytest.mark.parametrize("adapter", sorted(KNOWN_ADAPTERS))
def test_adapter_known_values_accepted(adapter):
    from deskbridge.config import ProjectConfig
    extra = {"openclaw_agent_id": "test-agent"} if adapter == "openclaw" else {}
    proj = ProjectConfig(
        id="p1", name="MyProj", repo_path="/repo",
        identity="alice", escalation_dm_target="npub1op",
        adapter=adapter,
        **extra,
    )
    assert proj.adapter == adapter


def test_adapter_unknown_value_raises_config_error(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG.rstrip() + '\nadapter = "voltron"\n')
    with pytest.raises(ConfigError, match="voltron"):
        load_config(cfg_file)


def test_openclaw_requires_agent_id(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG.rstrip() + '\nadapter = "openclaw"\n')
    with pytest.raises(ConfigError, match="openclaw_agent_id"):
        load_config(cfg_file)


def test_openclaw_with_agent_id_accepted(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(
        MINIMAL_CONFIG.rstrip() + '\nadapter = "openclaw"\nopenclaw_agent_id = "my-agent"\n'
    )
    cfg = load_config(cfg_file)
    assert cfg.projects[0].openclaw_agent_id == "my-agent"


def test_max_agent_attempts_default_is_3(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)
    config = load_config(cfg_file)
    assert config.projects[0].max_agent_attempts == 3


def test_max_agent_attempts_zero_raises(tmp_path):
    bad = MINIMAL_CONFIG.replace(
        'escalation_dm_target = "npub1human"',
        'escalation_dm_target = "npub1human"\nmax_agent_attempts = 0',
    )
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(bad)
    with pytest.raises(ConfigError):
        load_config(cfg_file)


def test_max_agent_attempts_custom_value_accepted(tmp_path):
    custom = MINIMAL_CONFIG.replace(
        'escalation_dm_target = "npub1human"',
        'escalation_dm_target = "npub1human"\nmax_agent_attempts = 5',
    )
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(custom)
    config = load_config(cfg_file)
    assert config.projects[0].max_agent_attempts == 5


def test_project_groups_default_empty(tmp_path):
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(MINIMAL_CONFIG)
    config = load_config(cfg_file)
    assert config.projects[0].groups == []


def test_project_groups_parsed_from_toml(tmp_path):
    custom = MINIMAL_CONFIG.replace(
        'escalation_dm_target = "npub1human"',
        'escalation_dm_target = "npub1human"\ngroups = ["grp-1", "grp-2"]',
    )
    cfg_file = tmp_path / "config.toml"
    cfg_file.write_text(custom)
    config = load_config(cfg_file)
    assert config.projects[0].groups == ["grp-1", "grp-2"]
