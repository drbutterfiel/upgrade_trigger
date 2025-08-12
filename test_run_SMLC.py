import numpy as np 
import math 
import os.path 
import glob
import h5py
import pandas as pd
import pickle
import matplotlib.colors as colors
import argparse
import os
import json
import gzip
import dask.dataframe as dd
from test_SMLC_pipeline import *
from icecube import icetray, dataio, dataclasses, simclasses, phys_services, trigger_sim

# Initiate the driver
driver = Driver()

# Path to your test file
test_files = ['/data/sim/IceCube/2023/generated/RandomNoise/23221/0000000-0000999/RandomNoise_IceCubeUpgrade_v58.23221.0.i3.zst']

# Process the file
driver.process_all_files(test_files)