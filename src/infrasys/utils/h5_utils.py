"""Utility functions for working with HDF5 files."""

from pathlib import Path
from typing import Literal, TypeAlias

import h5py

H5FileMode: TypeAlias = Literal["r", "r+", "a", "w", "w-"]


def copy_h5_group(src_group: h5py.Group, dst_group: h5py.Group) -> None:
    """Recursively copy HDF5 group contents using h5py public API.

    This function copies datasets and subgroups from a source HDF5 group to a
    destination HDF5 group, preserving the hierarchical structure and attributes.

    Parameters
    ----------
    src_group : h5py.Group
        Source HDF5 group to copy from
    dst_group : h5py.Group
        Destination HDF5 group to copy to

    Notes
    -----
    - Datasets are copied with their data, dtype, and chunk settings
    - Subgroups are recursively copied
    - All attributes from both datasets and groups are preserved
    """
    for key in src_group.keys():
        src_item = src_group[key]
        if isinstance(src_item, h5py.Dataset):
            # Copy dataset with only the essential properties
            dst_dataset = dst_group.create_dataset(
                key,
                data=src_item[()],
                dtype=src_item.dtype,
                chunks=src_item.chunks,
            )
            # Copy attributes
            for attr_key, attr_val in src_item.attrs.items():
                dst_dataset.attrs[attr_key] = attr_val
        elif isinstance(src_item, h5py.Group):
            # Recursively copy group
            dst_subgroup = dst_group.create_group(key)
            copy_h5_group(src_item, dst_subgroup)
            # Copy group attributes
            for attr_key, attr_val in src_item.attrs.items():
                dst_subgroup.attrs[attr_key] = attr_val


def extract_h5_dataset_to_bytes(group: h5py.Group | h5py.File, dataset_path: str) -> bytes:
    """Extract HDF5 dataset contents as bytes.

    Parameters
    ----------
    group : h5py.Group | h5py.File
        HDF5 group or file containing the dataset
    dataset_path : str
        Path to the dataset within the group

    Returns
    -------
    bytes
        Dataset contents as bytes

    Raises
    ------
    TypeError
        If the item at dataset_path is not a Dataset

    Notes
    -----
    This function is useful for extracting binary data like serialized databases
    from HDF5 files.
    """
    item = group[dataset_path]
    if isinstance(item, h5py.Dataset):
        return bytes(item[:])

    msg = f"Expected Dataset at {dataset_path!r}, got {type(item).__name__}"
    raise TypeError(msg)


def open_h5_file(file_path: Path | str, mode: str = "a") -> h5py.File:
    """Open an HDF5 file with string path conversion.

    Parameters
    ----------
    file_path : Path | str
        Path to the HDF5 file
    mode : str, optional
        File mode ('r', 'r+', 'a', 'w', 'w-'), by default 'a'

    Returns
    -------
    h5py.File
        Opened HDF5 file handle

    Notes
    -----
    - Accepts both Path and str objects
    - The file handle should be used with a context manager
    """
    return h5py.File(str(file_path), mode=mode)  # type: ignore[arg-type]
