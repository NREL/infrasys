import os

from infrasys.utils.path_utils import clean_tmp_folder, delete_if_exists


def test_delete_if_exists(tmp_path) -> None:
    assert not delete_if_exists(tmp_path / "non_existing")
    file_path = tmp_path / "file.txt"
    directory = tmp_path / "test_dir"
    assert not delete_if_exists(file_path)
    assert not delete_if_exists(directory)

    directory.mkdir()
    file_path.touch()
    for path in (directory, file_path):
        assert path.exists()
        assert delete_if_exists(path)
        assert not path.exists()


def test_clean_tmp_folder(tmp_path) -> None:
    nested = tmp_path / "keep_me" / "child"
    nested.mkdir(parents=True)
    (nested / "file.txt").write_text("data")

    clean_tmp_folder(tmp_path / "keep_me")
    assert not os.path.exists(tmp_path / "keep_me")
