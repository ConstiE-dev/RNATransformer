import pandas as pd
import os
import gc
from pathlib import Path
from tqdm import tqdm


def parse_pdb(filename):
    with open(filename, 'r') as file:
        records = [
            {
                'record_name': parts[0],
                'atom_serial': int(parts[1]),
                'atom_name': parts[2],
                'residue_name': parts[3],
                'chain_id': parts[4],
                'residue_seq': int(parts[5]),
                'x': float(parts[6]),
                'y': float(parts[7]),
                'z': float(parts[8]),
                'occupancy': float(parts[9]),
                'temp_factor': float(parts[10]),
                'element': parts[11]
            }
            for line in file if line.startswith("ATOM")
            for parts in [line.split()]
            if len(parts) >= 12
        ]
    return pd.DataFrame(records)

def process_pdb_file(filepath): 
    print(f"Processing {filepath}")
    file_name = os.path.basename(filepath)
    tmp = parse_pdb(filepath)
    tmp = tmp[["residue_name", "x", "y", "z"]].copy()
    tmp.loc[:, "resid"] = range(1, len(tmp) + 1)
    tmp = tmp.rename(columns= {'x': 'x_1', 'y': 'y_1', 'z' : 'z_1', 'residue_name': 'resname'})
    file_name =file_name.removesuffix(".pdb")
    tmp.loc[ : ,"sequence_id"] = file_name
    tmp.loc[:, "ID"] = [f"{file_name}_{i}" for i in range(1, len(tmp)+1)]
    return tmp
 
def create_input_df(target_df):
    df = target_df.groupby('sequence_id').apply(
        lambda group: ''.join(group.sort_values('resid')['resname'])
    ).reset_index(name='sequence')
    return df

def process_single_file(file):
    """Processes a single PDB file and returns the sequence and target DataFrames."""
    try:
        target_df = process_pdb_file(file)
        input_df = create_input_df(target_df)
        return input_df, target_df
    except Exception as e:
        print(f"Error processing file {file}: {e}")
        return None,None

def process_folder(folder_path, target_path):
    """Processes all PDB files in a folder and returns the combined sequence and target DataFrames."""
    folder_name = folder_path.split("\\")[-1]
    sequences = pd.DataFrame()
    targets = pd.DataFrame()
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)  
        sequence, target = process_single_file(file_path)
        sequences = pd.concat([sequences, sequence], ignore_index=True)
        targets = pd.concat([targets, target], ignore_index=True)
    sequences.to_pickle(f"{target_path}/sequences/sequences_{folder_name}.pkl")
    targets.to_pickle(f"{target_path}/targets/targets_{folder_name}.pkl")

def combine_pickle_files_in_batches(folder_path, batch_size=100, output_prefix="combined_batch"):
    """
    Read pickle files in a folder in batches and save each batch as a separate pickle file.
    
    Args:
        folder_path (str): Path to the folder containing pickle files
        batch_size (int): Number of files to process in each batch
        output_prefix (str): Prefix for the output pickle files
    """
    pickle_files = list(Path(folder_path).glob('*.pkl'))
    
    print(f"Found {len(pickle_files)} pickle files in {folder_path}")
    
    # Process files in batches
    for batch_idx, i in enumerate(range(0, len(pickle_files), batch_size)):
        batch_files = pickle_files[i:i+batch_size]
        
        print(f"Processing batch {batch_idx+1} ({len(batch_files)} files)")
        
        combined_df = pd.DataFrame()
        
        for file_path in tqdm(batch_files, desc=f"Batch {batch_idx+1}"):
            try:
                # Read the pickle file
                df = pd.read_pickle(file_path)
                
                # Concatenate to the combined DataFrame
                combined_df = pd.concat([combined_df, df], ignore_index=True)
                
            except Exception as e:
                print(f"Error processing {file_path}: {e}")
        
        # Save this batch as a pickle file
        output_file = f"../data/pickle/{output_prefix}_{batch_idx+1}.pkl"
        combined_df.to_pickle(output_file)
        print(f"Saved batch {batch_idx+1} with {len(combined_df)} rows to {output_file}")
        
        # Free memory
        del combined_df
        gc.collect()