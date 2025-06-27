import re
import numpy as np
import pandas as pd
import torch


def check_for_missing_values(sequences_df, targets_df):
    print(targets_df[targets_df.resname.isin(['X', '-'])])
    pattern = re.compile(r'[^GCAU]')
    print(sequences_df.loc[sequences_df['sequence'].apply(lambda x: bool(pattern.search(x)))])
    print(sequences_df.isna().sum())
    print(targets_df.isna().sum())

def convert_labels_to_dict(labels_df : pd.DataFrame) -> dict:
    """
    Processes a labels DataFrame by grouping rows by target_id.
    Returns a dictionary mapping target_id to an array of coordinates (seq_len, 3).
    """
    label_dict = {}
    for idx, row in labels_df.iterrows():
        print(f"Processing index: {idx}", end="\r")
        # Split ID into target_id and residue number (assumes format "targetid_resid")
        parts = row['ID'].split('_')
        target_id = "_".join(parts[:-1])
        resid = int(parts[-1])
        # Extract the coordinates; they should be numeric (missing values already set to 0)
        coord = np.array([row['x_1'], row['y_1'], row['z_1']], dtype=np.float32)
        if target_id not in label_dict:
            label_dict[target_id] = []
        label_dict[target_id].append((resid, coord))
    
    # Sort residues by resid and stack coordinates
    for key in label_dict:
        sorted_coords = sorted(label_dict[key], key=lambda x: x[0])
        coords = np.stack([c for r, c in sorted_coords])
        label_dict[key] = coords
    return label_dict

def convert_to_list_of_tensors(sequence_dict : dict, label_dict : dict):
    sequence_tensors = []
    labels_tensors = []

    for key in label_dict:
        sequence = sequence_dict[key]
        sequence_tensor = torch.tensor(sequence, dtype=torch.int32)
        sequence_tensors.append(sequence_tensor)   
        labels_tensor = torch.tensor(label_dict[key], dtype=torch.float32)
        labels_tensors.append(labels_tensor)

    return sequence_tensors, labels_tensors