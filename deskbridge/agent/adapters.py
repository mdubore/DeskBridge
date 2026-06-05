KNOWN_ADAPTERS = {"claude-code", "codex", "gemini"}


def build_command(adapter: str, repo_path: str, prompt: str) -> list[str]:
    if adapter == "claude-code":
        return ["claude", "--project", repo_path, "--message", prompt]
    elif adapter == "codex":
        return ["codex", "--cwd", repo_path, "--no-interactive", prompt]
    elif adapter == "gemini":
        return ["gemini", "--yolo", "-p", prompt]
    raise ValueError(f"Unknown adapter: {adapter!r}")
