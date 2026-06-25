# Releasing `undr9`

This document describes the release flow for the standalone `undr9-python` repository.

## Package Identity

- PyPI package name: `undr9`
- Install command:

```bash
pip install undr9
```

## Immediate Security Follow-Up

The first publish was completed with a manually pasted PyPI token. Rotate that token in PyPI before
using this release process again.

Recommended follow-up:

1. Revoke the old token in the PyPI account settings.
2. Create a new scoped token if you still need a manual fallback path.
3. Prefer PyPI Trusted Publishing for all future releases.

## Release Workflow In This Repo

The checked-in release workflow is:

- [python-sdk-release.yml](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9-python/.github/workflows/python-sdk-release.yml)

It currently:

- builds the wheel and sdist
- runs `twine check`
- verifies installation from built artifacts
- attaches artifacts to a GitHub release
- publishes to PyPI through `pypa/gh-action-pypi-publish`

## Recommended Future Release Flow

1. Update version in `pyproject.toml`.
2. Run local verification:

```bash
python3 -m venv .venv-publish
source .venv-publish/bin/activate
python -m pip install --upgrade pip build twine
python -m build
python -m twine check dist/*
python scripts/verify_dist.py
python -m unittest -q tests.test_sdk
```

3. Commit the release changes.
4. Create and push a tag:

```bash
git tag python-sdk-v0.1.1
git push origin python-sdk-v0.1.1
```

5. Create a GitHub release for that tag, or use the workflow dispatch path.
6. Let GitHub Actions publish to PyPI through Trusted Publishing.

## PyPI Trusted Publishing Setup

Configure this once in PyPI for the `undr9` project.

### In GitHub

- Repository: `undr9/undr9-python`
- Workflow file: `python-sdk-release.yml`

### In PyPI

Add a Trusted Publisher with:

- owner: `undr9`
- repository: `undr9-python`
- workflow name or file: `python-sdk-release.yml`
- environment: leave empty unless you explicitly gate releases with a GitHub environment

After that, GitHub Actions can publish without storing a PyPI token in repository secrets.

## Manual Fallback Publish

Only use this if Trusted Publishing is unavailable.

```bash
source .venv-publish/bin/activate
export TWINE_USERNAME=__token__
export TWINE_PASSWORD='<new-pypi-token>'
python -m twine upload dist/*
```

Do not paste tokens into issue trackers, chat logs, screenshots, or reusable shell history.

## Release Checklist

- version bumped in `pyproject.toml`
- README and examples reflect the release
- `python -m build` passes
- `python -m twine check dist/*` passes
- `python scripts/verify_dist.py` passes
- `python -m unittest -q tests.test_sdk` passes
- optional live contract test passes
- tag pushed
- GitHub release created
- PyPI page confirms the new version
- fresh install works:

```bash
python3 -m venv /tmp/undr9-pypi-check
source /tmp/undr9-pypi-check/bin/activate
pip install --upgrade pip
pip install undr9
python -c "import undr9, importlib.metadata; print(importlib.metadata.version('undr9'))"
```
