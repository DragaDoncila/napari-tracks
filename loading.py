import napari
import pandas as pd
import numpy as np


with napari.gui_qt():
    viewer = napari.Viewer()
    viewer.open("/media/draga/Elements/CellTracking/tracks.csv")