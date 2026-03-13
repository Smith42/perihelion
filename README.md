---
title: Perihelion
emoji: 🌌
colorFrom: blue
colorTo: pink
sdk: docker
app_file: app.py
pinned: false
short_description: Which galaxy is right for you?
---

# Perihelion

Vote on pairs of galaxy images to build a community ELO ranking of the most interesting galaxies.

## Environment variables

### Local development

Create a `.env` file in the project root (never commit this):

```
# HuggingFace — required for persistent ELO state across restarts
HF_TOKEN=hf_...
HF_LOG_REPO_ID=your-username/perihelion-logs

# Optional
HF_LOG_EVERY_MINUTES=10

# Dataset
DATASET_ID=mwalmsley/gz_euclid
DATASET_CONFIG=default
DATASET_SPLIT=train
IMAGE_COLUMN=image
ID_COLUMN=id_str
POOL_SIZE=5000
POOL_SEED=42

# Image cache
IMAGE_CACHE_DIR=cache/images
IMAGE_CACHE_MAX_BYTES=524288000
```

Then run:

```bash
uv run python app.py
```

### HuggingFace Spaces

Set variables in **Settings → Variables and secrets** for your Space:

| Variable | Where to set | Description |
|---|---|---|
| `HF_TOKEN` | **Secrets** | HuggingFace token with read/write access to `HF_LOG_REPO_ID` |
| `HF_LOG_REPO_ID` | Variables | Dataset repo for persisting ELO state, e.g. `your-username/perihelion-logs` |
| `HF_LOG_EVERY_MINUTES` | Variables | How often to sync state to HF Hub (default: `10`) |
| `DATASET_ID` | Variables | HF dataset to sample galaxies from (default: `mwalmsley/gz_euclid`) |
| `DATASET_CONFIG` | Variables | Dataset config name (default: `default`) |
| `DATASET_SPLIT` | Variables | Dataset split (default: `train`) |
| `IMAGE_COLUMN` | Variables | Name of the image column (default: `image`) |
| `ID_COLUMN` | Variables | Name of the ID column for display names (default: `id_str`) |
| `POOL_SIZE` | Variables | Number of galaxies to sample (default: `5000`) |
| `POOL_SEED` | Variables | Shuffle seed — keep this fixed so all participants see the same pool (default: `42`) |
| `IMAGE_CACHE_MAX_BYTES` | Variables | Max disk space for image cache in bytes (default: `524288000` = 500 MB) |

> **Note:** `HF_TOKEN` must be added as a **Secret** (not a variable) to keep it private.

### Persistent ELO state

ELO scores are written to `state/elo_state.json` on every comparison and periodically synced to HF Hub. The saved state is matched to `DATASET_ID` — if you change the dataset, old state is automatically discarded and a fresh ranking starts.

Without `HF_LOG_REPO_ID` set, state is only saved locally and will be lost on container restart.
