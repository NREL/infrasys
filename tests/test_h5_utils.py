"""Tests for HDF5 utility functions."""

import h5py
import numpy as np
import pytest

from infrasys.utils.h5_utils import copy_h5_group, extract_h5_dataset_to_bytes, open_h5_file


@pytest.fixture
def h5_file_with_data(tmp_path):
    """Create a temporary HDF5 file with test data."""
    file_path = tmp_path / "test.h5"
    with h5py.File(str(file_path), "w") as f:
        # Create a dataset
        data = np.arange(100)
        f.create_dataset("data", data=data)
        f["data"].attrs["description"] = "Test data"

        # Create a group with nested data
        group = f.create_group("group1")
        group.create_dataset("nested_data", data=np.arange(50))
        group["nested_data"].attrs["type"] = "nested"
        group.attrs["group_attr"] = "group value"

        # Create a subgroup
        subgroup = group.create_group("subgroup")
        subgroup.create_dataset("deep_data", data=np.array([1, 2, 3]))

    return file_path


def test_open_h5_file_with_path_object(tmp_path):
    """Test opening HDF5 file with Path object."""
    file_path = tmp_path / "test.h5"

    # Create and close file first
    with h5py.File(str(file_path), "w") as f:
        f.create_dataset("data", data=[1, 2, 3])

    # Test opening with Path object
    f = open_h5_file(file_path, mode="r")
    assert isinstance(f, h5py.File)
    assert "data" in f
    f.close()


def test_open_h5_file_with_string_path(tmp_path):
    """Test opening HDF5 file with string path."""
    file_path = str(tmp_path / "test.h5")

    # Create and close file first
    with h5py.File(file_path, "w") as f:
        f.create_dataset("data", data=[1, 2, 3])

    # Test opening with string path
    f = open_h5_file(file_path, mode="r")
    assert isinstance(f, h5py.File)
    assert "data" in f
    f.close()


def test_open_h5_file_create_mode(tmp_path):
    """Test opening HDF5 file in create mode."""
    file_path = tmp_path / "new.h5"

    f = open_h5_file(file_path, mode="w")
    assert isinstance(f, h5py.File)
    f.create_dataset("test", data=[1, 2, 3])
    f.close()

    assert file_path.exists()


def test_extract_h5_dataset_to_bytes(h5_file_with_data):
    """Test extracting dataset as bytes."""
    with h5py.File(str(h5_file_with_data), "r") as f:
        result = extract_h5_dataset_to_bytes(f, "data")

        assert isinstance(result, bytes)
        # Verify the data is correct
        data = np.frombuffer(result, dtype=np.int64)
        assert np.array_equal(data, np.arange(100))


def test_extract_h5_dataset_preserves_attributes(h5_file_with_data):
    """Test that extracted dataset respects attributes."""
    with h5py.File(str(h5_file_with_data), "r") as f:
        result = extract_h5_dataset_to_bytes(f, "data")
        assert isinstance(result, bytes)


def test_extract_h5_dataset_not_found(h5_file_with_data):
    """Test extracting non-existent dataset raises error."""
    with h5py.File(str(h5_file_with_data), "r") as f:
        with pytest.raises(KeyError):
            extract_h5_dataset_to_bytes(f, "nonexistent")


def test_extract_h5_dataset_wrong_type(h5_file_with_data):
    """Test extracting group instead of dataset raises TypeError."""
    with h5py.File(str(h5_file_with_data), "r") as f:
        with pytest.raises(TypeError, match="Expected Dataset"):
            extract_h5_dataset_to_bytes(f, "group1")


def test_copy_h5_group_single_dataset(tmp_path):
    """Test copying a group with a single dataset."""
    src_file = tmp_path / "src.h5"
    dst_file = tmp_path / "dst.h5"

    # Create source
    with h5py.File(str(src_file), "w") as src:
        src.create_dataset("data", data=np.arange(10))
        src["data"].attrs["attr1"] = "value1"

        with h5py.File(str(dst_file), "w") as dst:
            copy_h5_group(src, dst)

    # Verify copy
    with h5py.File(str(dst_file), "r") as dst:
        assert "data" in dst
        data_set = dst["data"]
        assert isinstance(data_set, h5py.Dataset)
        assert np.array_equal(data_set[()], np.arange(10))
        assert data_set.attrs["attr1"] == "value1"


def test_copy_h5_group_nested_structure(tmp_path):
    """Test copying nested group structure."""
    src_file = tmp_path / "src.h5"
    dst_file = tmp_path / "dst.h5"

    # Create source with nested structure
    with h5py.File(str(src_file), "w") as src:
        src.create_dataset("root_data", data=[1, 2, 3])

        group1 = src.create_group("group1")
        group1.create_dataset("nested_data", data=[4, 5, 6])
        group1.attrs["group_attr"] = "test"

        subgroup = group1.create_group("subgroup")
        subgroup.create_dataset("deep_data", data=[7, 8, 9])

        with h5py.File(str(dst_file), "w") as dst:
            copy_h5_group(src, dst)

    # Verify nested copy
    with h5py.File(str(dst_file), "r") as dst:
        assert "root_data" in dst
        root_data = dst["root_data"]
        assert isinstance(root_data, h5py.Dataset)
        assert np.array_equal(root_data[()], [1, 2, 3])

        assert "group1" in dst
        group1_obj = dst["group1"]
        assert isinstance(group1_obj, h5py.Group)
        assert "nested_data" in group1_obj
        nested = group1_obj["nested_data"]
        assert isinstance(nested, h5py.Dataset)
        assert np.array_equal(nested[()], [4, 5, 6])
        assert group1_obj.attrs["group_attr"] == "test"

        assert "subgroup" in group1_obj
        subgroup_obj = group1_obj["subgroup"]
        assert isinstance(subgroup_obj, h5py.Group)
        assert "deep_data" in subgroup_obj
        deep_data = subgroup_obj["deep_data"]
        assert isinstance(deep_data, h5py.Dataset)
        assert np.array_equal(deep_data[()], [7, 8, 9])


def test_copy_h5_group_multiple_datasets(tmp_path):
    """Test copying group with multiple datasets."""
    src_file = tmp_path / "src.h5"
    dst_file = tmp_path / "dst.h5"

    # Create source
    with h5py.File(str(src_file), "w") as src:
        src.create_dataset("data1", data=np.arange(5))
        src.create_dataset("data2", data=np.arange(10, 20))
        src.create_dataset("data3", data=["a", "b", "c"])

        with h5py.File(str(dst_file), "w") as dst:
            copy_h5_group(src, dst)

    # Verify all datasets copied
    with h5py.File(str(dst_file), "r") as dst:
        assert len(dst.keys()) == 3
        data1 = dst["data1"]
        assert isinstance(data1, h5py.Dataset)
        assert np.array_equal(data1[()], np.arange(5))

        data2 = dst["data2"]
        assert isinstance(data2, h5py.Dataset)
        assert np.array_equal(data2[()], np.arange(10, 20))

        data3 = dst["data3"]
        assert isinstance(data3, h5py.Dataset)
        assert np.array_equal(data3[()], np.array([b"a", b"b", b"c"]))


def test_copy_h5_group_preserves_dataset_attributes(tmp_path):
    """Test that copying preserves all dataset attributes."""
    src_file = tmp_path / "src.h5"
    dst_file = tmp_path / "dst.h5"

    # Create source with attributes
    with h5py.File(str(src_file), "w") as src:
        dset = src.create_dataset("data", data=[1, 2, 3, 4, 5])
        dset.attrs["description"] = "Test dataset"
        dset.attrs["version"] = 1
        dset.attrs["tags"] = np.array([10, 20, 30])

        with h5py.File(str(dst_file), "w") as dst:
            copy_h5_group(src, dst)

    # Verify attributes
    with h5py.File(str(dst_file), "r") as dst:
        dst_data = dst["data"]
        assert isinstance(dst_data, h5py.Dataset)
        assert dst_data.attrs["description"] == "Test dataset"
        assert dst_data.attrs["version"] == 1
        assert np.array_equal(np.asarray(dst_data.attrs["tags"]), np.array([10, 20, 30]))


def test_copy_h5_group_preserves_group_attributes(tmp_path):
    """Test that copying preserves group attributes."""
    src_file = tmp_path / "src.h5"
    dst_file = tmp_path / "dst.h5"

    # Create source with group attributes
    with h5py.File(str(src_file), "w") as src:
        group = src.create_group("mygroup")
        group.create_dataset("data", data=[1, 2, 3])
        group.attrs["group_name"] = "My Group"
        group.attrs["group_id"] = 42

        with h5py.File(str(dst_file), "w") as dst:
            copy_h5_group(src, dst)

    # Verify group attributes
    with h5py.File(str(dst_file), "r") as dst:
        mygroup = dst["mygroup"]
        assert isinstance(mygroup, h5py.Group)
        assert mygroup.attrs["group_name"] == "My Group"
        assert mygroup.attrs["group_id"] == 42


def test_copy_h5_group_empty_group(tmp_path):
    """Test copying an empty group."""
    src_file = tmp_path / "src.h5"
    dst_file = tmp_path / "dst.h5"

    # Create source with empty group
    with h5py.File(str(src_file), "w") as src:
        src.create_group("empty_group")

        with h5py.File(str(dst_file), "w") as dst:
            copy_h5_group(src, dst)

    # Verify empty group exists
    with h5py.File(str(dst_file), "r") as dst:
        assert "empty_group" in dst
        empty_group = dst["empty_group"]
        assert isinstance(empty_group, h5py.Group)
        assert len(empty_group.keys()) == 0


def test_copy_h5_group_with_chunks(tmp_path):
    """Test copying chunked datasets preserves chunk settings."""
    src_file = tmp_path / "src.h5"
    dst_file = tmp_path / "dst.h5"

    # Create source with chunked dataset
    with h5py.File(str(src_file), "w") as src:
        src.create_dataset("chunked", data=np.arange(1000), chunks=(100,))

        with h5py.File(str(dst_file), "w") as dst:
            copy_h5_group(src, dst)

    # Verify chunks are preserved
    with h5py.File(str(dst_file), "r") as dst:
        chunked = dst["chunked"]
        assert isinstance(chunked, h5py.Dataset)
        assert chunked.chunks == (100,)
        assert np.array_equal(chunked[()], np.arange(1000))
