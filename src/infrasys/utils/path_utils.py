import shutil
from pathlib import Path


def delete_if_exists(path: Path) -> None:
    """Delete a file or directory if it exists."""
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
