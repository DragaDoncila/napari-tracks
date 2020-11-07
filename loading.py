import napari
import pandas as pd
import numpy as np


with napari.gui_qt():
    viewer = napari.Viewer()
    viewer.open("./napari_tracks/_tests/tracks.csv")