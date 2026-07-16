# Continue Claude Code

![Claude usage limit reset](assets/usage-limit-reset.png)

Queue a prompt for an existing Claude Code session, choose when to run it, and leave it working while your computer stays awake. `ccc` automatically finds local Claude sessions and their project folders on macOS, Linux, and Windows.

```bash
brew tap egesecgin/ccc
brew install ccc
ccc
```

Press `n`, pick a session with `F2`, write the prompt, choose a time, then press `Ctrl+Enter`. Keep the app open, or run `ccc --worker` without the interface.

`ccc` uses your installed `claude` command to continue the selected session. It does not bypass usage limits, sign-in, or Claude permission prompts.
