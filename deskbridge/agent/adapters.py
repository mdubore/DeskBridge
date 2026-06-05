KNOWN_ADAPTERS = {"claude-code", "codex", "gemini", "hermes", "openclaw"}


def build_command(adapter: str, repo_path: str, prompt: str, *, agent_id: str | None = None) -> list[str]:
    if adapter == "claude-code":
        return ["claude", "--project", repo_path, "--message", prompt]
    elif adapter == "codex":
        return ["codex", "--cwd", repo_path, "--no-interactive", prompt]
    elif adapter == "gemini":
        # repo_path passed as cwd= on subprocess
        return ["gemini", "--yolo", "-p", prompt]
    elif adapter == "hermes":
        # repo_path passed as cwd= on subprocess; -Q suppresses banner/spinner
        return ["hermes", "chat", "-Q", "-q", prompt]
    elif adapter == "openclaw":
        # workspace is pre-configured per agent at setup time (no per-invocation flag)
        return ["openclaw", "agent", "--agent", agent_id, "--local", "--message", prompt]
    raise ValueError(f"Unknown adapter: {adapter!r}")
