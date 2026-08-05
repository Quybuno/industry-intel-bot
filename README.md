# Industry Intelligence Bot

Minimal scaffold for the Industry Intelligence Bot. Contains ingest → filter → score → publish pipeline.

Quick start (dev):

1. Create virtualenv and install dependencies:

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -U pip
python -m pip install -e .
```

2. Copy `.env.example` → `.env` and edit values.

3. Run basic CLI (placeholders):

```powershell
python -m src.intel_bot.cli ingest
```

This repo is under active development; many commands are scaffolds. Follow `docs/PRODUCTION_PLAN.md` for design guidance.
