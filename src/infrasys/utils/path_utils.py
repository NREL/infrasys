import shutil
from pathlib import Path


def delete_if_exists(path: Path) -> bool:
    """Delete a file or directory if it exists.

    Parameters
    ----------
    path : Path
        The path to the file or directory to delete.

    Returns
    -------
    bool
        True if the file or directory was deleted, False otherwise.
    """
    if path.exists():
        if path.is_dir():
            shutil.rmtree(path)
        else:
            path.unlink()
        return True
    return False
