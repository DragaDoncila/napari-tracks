import numpy as np
from napari_plugin_engine import napari_hook_implementation
import time
import pandas as pd


def get_tracks(
    df,
    min_frames=0,
    id_col="particle",
    time_col="frame",
    coord_cols=("x", "y", "z"),
    scale=(1, 1, 1),
    w_prop=True,
):
    """
    Get the tracks from pandas.DataFrame containing object ID,
    time, and coordinates for viewing in napari. Filters tracks
    according to a specified or default minimum number of frames
    in which they should appear. Returns tuple containing an
    array with columns ID, t, <coord cols> and the properties
    dictionary.
    Parameters
    ----------
    df: pd.DataFrame
        tracks data
    min_frames: int
        minimum frames in which an object should appear
        to be added to the tracks data for viewing.
    id_col: str
        The name of the column in which object ID is found
    time_col: str
        The name of the column in which the time is found
    coord_cols: tuple or list of str
        names of columns containing the object coordinates
    scale: numeric or tuple or string of numeric
        To scale coordinates for viewing with the data

    Returns
    -------
    track_data: np.ndarray
        array with cols ID, t, <coords>
        The data is sorted by ID then t in accordance
        with napari tracks data validation
    dict(df_filtered): dict
        dict containing the properties for napari
    """
    time_0 = time.time()
    id_array = df[id_col].to_numpy()
    track_count = np.bincount(id_array)
    df["track length"] = track_count[id_array]
    df_filtered = df.loc[df["track length"] >= min_frames, :]
    df_filtered = df_filtered.sort_values(by=[id_col, time_col])
    data_cols = [id_col, time_col] + list(coord_cols)
    track_data = df_filtered[data_cols].to_numpy()
    track_data[:, -3:] *= scale
    print(
        f"{np.sum(track_count >= min_frames)} tracks found in "
        f"{time.time() - time_0} seconds"
    )
    if w_prop:
        return track_data, df_filtered.to_dict('list')
    else:
        return track_data


@napari_hook_implementation
def napari_get_reader(path):
    """Checks the file extension of path and returns True if the path (or all paths in list)
    is a CSV file, otherwise False

    Parameters
    ----------
    path : str
        Path to file, or list of paths.

    Returns
    -------
    function or None
        If the path is a recognized format, return a function that accepts the
        same path or list of paths, and returns a list of layer data tuples.
    """
    # all paths in list must be CSVs
    if isinstance(path, list):
        return reader_function if all([pth.endswith('.csv') for pth in path]) else None

    # just one string, must be CSV
    if not path.endswith(".csv"):
        return None

    # otherwise we return the *function* that can read ``path``.
    return reader_function


def reader_function(path):
    """Read in tracks data and return tracks layer(s)

    Parameters
    ----------
    path : str or list of str
        Path to file, or list of paths.

    Returns
    -------
    layer_data : list of tuples
        A list of LayerData tuples where each tuple in the list contains
        (data, metadata, layer_type), where data is a numpy array, metadata is
        a dict of keyword arguments for the corresponding viewer.add_* method
        in napari, and layer_type is a lower-case string naming the type of layer.
        Both "meta", and "layer_type" are optional. napari will default to
        layer_type=="image" if not provided
    """
    # handle both a string and a list of strings
    paths = [path] if isinstance(path, str) else path
    
    layer_type = 'tracks'
    layer_list = []
    for path in paths:
        tracks_df = pd.read_csv(path)
        tracks, properties = get_tracks(tracks_df)
        add_kwargs = {
            'properties':properties,
            'color_by':'particle',
            'colormap': 'viridis',
        }
        layer_list.append(
            (tracks, add_kwargs, layer_type)
        )
        
    return layer_list