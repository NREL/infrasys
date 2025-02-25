from infrasys.utils.path_utils import delete_if_exists


def test_delete_if_exists(tmp_path) -> None:
    delete_if_exists(tmp_path / "non_existing")
    file_path = tmp_path / "file.txt"
    directory = tmp_path / "test_dir"
    delete_if_exists(file_path)
    delete_if_exists(directory)

    directory.mkdir()
    file_path.touch()
    for path in (directory, file_path):
        assert path.exists()
        delete_if_exists(path)
        assert not path.exists()
