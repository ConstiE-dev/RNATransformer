import pandas as pd
import plotly.graph_objects as go

def plot_structure(df: pd.DataFrame, sequence_id: str) -> None:
    sequence_df = df[df["sequence_id"] == sequence_id]
    sequence_points = sequence_df[["x_1", "y_1", "z_1", "resname"]]
    
    colors = {"A": "red", "G": "blue", "C": "green", "U": "orange"}
    fig = go.Figure()
    
    for resname, color in colors.items():
        subset = sequence_df[sequence_df["resname"] == resname]
        fig.add_trace(go.Scatter3d(
            x=subset["x_1"], y=subset["y_1"], z=subset["z_1"],
            mode='markers',
            marker=dict(size=5, color=color),
            name=resname,
        ))
    
    fig.add_trace(go.Scatter3d(
        x=sequence_df["x_1"], y=sequence_df["y_1"], z=sequence_df["z_1"],
        mode='lines',
        line=dict(color='gray', width=2),
        name='RNA Backbone'
    ))
    
    fig.update_layout(
            scene=dict(xaxis_title='X', yaxis_title='Y', zaxis_title='Z'),
            title=f'3D RNA Structure of sequence {sequence_id}',
        )
            
    fig.show()