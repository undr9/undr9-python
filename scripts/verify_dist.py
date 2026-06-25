from __future__ import annotations

import subprocess
import sys
import tempfile
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DIST_DIR = ROOT / "dist"


def _python_in_venv(venv_dir: Path) -> Path:
    if sys.platform == "win32":
        return venv_dir / "Scripts" / "python.exe"
    return venv_dir / "bin" / "python"


def _run(command: list[str]) -> None:
    subprocess.run(command, check=True)


def _verify_artifact(artifact: Path, work_dir: Path) -> None:
    venv_dir = work_dir / artifact.name.replace(".", "_")
    _run([sys.executable, "-m", "venv", str(venv_dir)])
    python = _python_in_venv(venv_dir)

    _run([str(python), "-m", "pip", "install", "--upgrade", "pip"])
    _run([str(python), "-m", "pip", "install", str(artifact)])
    _run(
        [
            str(python),
            "-c",
            (
                "import importlib.metadata; "
                "import undr9; "
                "from undr9 import AsyncUndr9Client, PropertyValue, SyncUndr9Client; "
                "assert hasattr(undr9, '__all__'); "
                "assert importlib.metadata.version('undr9'); "
                "assert SyncUndr9Client is not None; "
                "assert AsyncUndr9Client is not None; "
                "assert PropertyValue.string('ok').value == 'ok'"
            ),
        ]
    )


def main() -> int:
    artifacts = sorted(DIST_DIR.glob("*.whl")) + sorted(DIST_DIR.glob("*.tar.gz"))
    if not artifacts:
        raise SystemExit(f"no build artifacts found in {DIST_DIR}")

    with tempfile.TemporaryDirectory(prefix="undr9-sdk-dist-") as temp_dir:
        work_dir = Path(temp_dir)
        for artifact in artifacts:
            _verify_artifact(artifact, work_dir)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
