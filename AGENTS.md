# Agent Scope Rules

## Repository Boundary
- Only consider files and directories inside `/Users/oskar.hinojosa/Documents/Test/Migrate`.
- Treat this `Migrate` directory as the full project boundary.
- Do not read, analyze, or modify content outside this directory unless explicitly requested by the user.

## Ignore Rules
- Do not use or rely on files ignored by Git.
- Respect `.gitignore` rules found in this repository (including nested `.gitignore` files).
- Skip ignored files for discovery, analysis, summaries, and code changes.
