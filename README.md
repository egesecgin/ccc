# Continue Claude Code

![Claude usage limit reset](assets/usage-limit-reset.png)

Queue a prompt for an existing Claude Code session, choose when to run it, and leave it working while your computer stays awake. `ccc` automatically finds local Claude sessions and their project folders on macOS, Linux, and Windows.

```bash
brew tap egesecgin/ccc
brew install ccc
ccc
```

On Linux or Windows with Rust installed:

```bash
cargo install --git https://github.com/egesecgin/ccc --tag v0.1.0
ccc
```

The session list is read-only. Press `n` or `Enter`, choose a session, write the prompt, choose a time, then press `Ctrl+Enter`. `x` only cancels a queued order after confirmation; it never deletes a Claude session. Keep the app open, or run `ccc --worker` without the interface.

`ccc` uses your installed `claude` command to continue the selected session. It does not bypass usage limits, sign-in, or Claude permission prompts.
