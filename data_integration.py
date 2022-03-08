import os
import glob

import pandas as pd


def integrate(filelist_path, meta_path, feature_path, output_path):
    filelist_df = pd.read_csv(filelist_path)

    feature_df = pd.read_csv(feature_path)
    altm_df = pd.read_json(meta_path, lines=True)

    df = filelist_df.merge(feature_df, on="AccessionID")
    full_df = df.merge(altm_df, left_on='PMID', right_on='meta_id')

    full_df.to_csv(output_path)