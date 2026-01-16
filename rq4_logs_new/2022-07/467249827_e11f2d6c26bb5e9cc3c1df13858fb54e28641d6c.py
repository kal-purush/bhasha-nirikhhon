import os.path

import dash_bootstrap_components as dbc
import hdbscan
import numba
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import umap
from dash import Dash, dcc, html, Input, Output
from rdkit import Chem
from rdkit.Chem import AllChem
from rdkit.Chem import DataStructs
from sklearn.metrics import pairwise_distances

from lsal.utils import get_basename, file_exists, removefile
from lsal.utils import json_load, scale_df, SEED

SUGGEST_K = 200
SUGGEST_TOP = True
COMPLEXITY_INDEX = 0
COMPLEXITY_PERCENTILE = 25
NN = 5
MD = 0.2
MCS = 8
MS = 10

_data = json_load("suggest_data.json")
_dfs = _data["dfs"]
_smiles_data = _data["smiles_data"]
_available_fom = ("fom2", "fom3")
_available_metric = ('mu-top2%', 'std-top2%mu')
_complexity_descriptors = ["sa_score", "BertzCT"]
_distance_metric = "manhattan"


def get_img_str(smi: str):
    return _smiles_data[smi]["img"]


@numba.njit()
def tanimoto_dist(a, b):
    dotprod = np.dot(a, b)
    tc = dotprod / (np.sum(a) + np.sum(b) - dotprod)
    return 1.0 - tc


def get_pool_ligands_to_records():
    pool_ligands = []
    des_records = []
    df_inv = pd.read_csv("../../../Screening/results/05_summary/inv.csv")
    df_des = pd.read_csv("../../../Screening/results/05_summary/des.csv")
    for i, (inv_r, des_r) in enumerate(
            zip(
                df_inv.to_dict(orient="records"),
                df_des.to_dict(orient="records"),
            )
    ):
        ligand = inv_r["smiles"]
        pool_ligands.append(ligand)
        des_records.append(des_r)
    ligand_to_des_record = dict(zip(pool_ligands, des_records))
    print("# of pool ligands: {}".format(len(ligand_to_des_record)))
    return ligand_to_des_record


SmiToRecords = get_pool_ligands_to_records()


def suggest_from_predictions(
        fom_type: str, metric: str, k: int, top: bool = True,
        complexity_type: str = _complexity_descriptors[0], complexity_cutoff: float = 25
):
    df = _dfs[fom_type][["smiles", metric]]
    df = df.assign(**{complexity_type: [_smiles_data[smi][complexity_type] for smi in df["smiles"]]})

    comp_values = df[complexity_type]
    complexity_cutoff = float(np.percentile(comp_values, complexity_cutoff))

    df: pd.DataFrame
    records = df.to_dict(orient="records")
    records = sorted([r for r in records if r[complexity_type] <= complexity_cutoff], key=lambda x: x[metric],
                     reverse=top)[:k]
    return pd.DataFrame.from_records(records)


def distmat_features(smis: list[str]):
    records = [SmiToRecords[smi] for smi in smis]
    df = pd.DataFrame.from_records(records)
    df = scale_df(df)
    distance_matrix = pairwise_distances(df.values, metric=_distance_metric)
    return distance_matrix


def distmat_fps(smiles: list[str]):
    mols = [Chem.MolFromSmiles(smi) for smi in smiles]
    # get fingerprints
    X = []
    for mol in mols:
        arr = np.zeros((0,))
        fp = AllChem.GetMorganFingerprintAsBitVect(mol, 2)
        DataStructs.ConvertToNumpyArray(fp, arr)
        X.append(arr)
    return pairwise_distances(X, metric=tanimoto_dist)


def make_suggestions(
        fom_type: str = _available_fom[0], metric: str = _available_metric[0], k: int = SUGGEST_K,
        top: bool = SUGGEST_TOP,
        complexity_type: str = _complexity_descriptors[COMPLEXITY_INDEX],
        complexity_cutoff: float = COMPLEXITY_PERCENTILE,
        nn=NN, md=MD, mcs=MCS, ms=MS
):
    suggest_df = suggest_from_predictions(
        fom_type, metric, k, top, complexity_type, complexity_cutoff
    )

    dmat_fe = distmat_features(suggest_df["smiles"])
    dmat_fp = distmat_fps(suggest_df["smiles"])

    dimred_transformer = umap.UMAP(
        n_neighbors=nn, min_dist=md, metric="precomputed", random_state=SEED)
    dimred_fe = dimred_transformer.fit_transform(dmat_fe)
    dimred_fp = dimred_transformer.fit_transform(dmat_fp)

    hdb = hdbscan.HDBSCAN(min_cluster_size=mcs, min_samples=ms, gen_min_span_tree=False)
    hdb.fit(dimred_fe)
    fe_labels = [lab for lab in hdb.labels_]
    hdb.fit(dimred_fp)
    fp_labels = [lab for lab in hdb.labels_]
    return suggest_df, dimred_fp, dimred_fe, fp_labels, fe_labels


app = Dash(external_stylesheets=[dbc.themes.BOOTSTRAP])

app.layout = html.Div([
    html.Div([
        html.Div([
            html.P("FOM:"),
            dcc.Dropdown(_available_fom, _available_fom[0], id='fom_type'),
        ], style={'width': '15%', 'display': 'inline-block'}),
        html.Div([
            html.P("y axis:"),
            dcc.Dropdown(_complexity_descriptors, _complexity_descriptors[0], id='ydef'),
        ], style={'width': '15%', 'display': 'inline-block'}),
        html.Div([
            html.P("x axis:"),
            dcc.Dropdown(_available_metric, _available_metric[0], id='xdef'),
        ], style={'width': '15%', 'display': 'inline-block'}),
        html.Div([
            html.P("suggest k:"),
            dcc.Input(200, type="number", id="suggest_k")
        ], style={'width': '15%', 'display': 'inline-block'}),
        html.Div([
            html.P("complexity percentile cutoff:"),
            dcc.Input(25, type="number", id="comp_cutoff")
        ], style={'width': '15%', 'display': 'inline-block'}),
        html.Div([
            html.P("suggest from:"),
            dcc.Dropdown(["+x", "-x"], "+x", id='suggest_top'),
        ], style={'width': '15%', 'display': 'inline-block'}),
        html.Div([
            html.P("show what:"),
            dcc.Dropdown(['dimred_FP', 'dimred_FE', 'xy'], 'xy', id='what'),
        ], style={'width': '15%', 'display': 'inline-block'}),
        html.Div([
            html.P("dimred param NN (3-9):"),
            dcc.Input(5, type="number", id="dimred_nn"),
        ], style={'width': '15%', 'display': 'inline-block'}),
        html.Div([
            html.P("dimred param MD (0.1-1):"),
            dcc.Input(0.2, type="number", id="dimred_md"),
        ], style={'width': '15%', 'display': 'inline-block'}),
        html.Div([
            html.P("dimred param min_cluster_size:"),
            dcc.Input(5, type="number", id="cluster_mcs"),
        ], style={'width': '15%', 'display': 'inline-block'}),
        html.Div([
            html.P("dimred param min_samples:"),
            dcc.Input(10, type="number", id="cluster_ms"),
        ], style={'width': '15%', 'display': 'inline-block'}),

    ]),

    dcc.Graph(id='graph'),

    html.Div(
        [
            html.Button("Download CSV", id="btn_csv"),
            dcc.Download(id="download-dataframe-csv"),
        ]
    ),
    html.Div(children=[],
             style={'margin-right': '90px', 'margin-left': '90px'},
             id="ligand_table"
             )

])


@app.callback(
    Output('graph', 'figure'),
    [
        Input('fom_type', 'value'),
        Input('xdef', 'value'),
        Input('ydef', 'value'),
        Input('suggest_k', 'value'),
        Input('comp_cutoff', 'value'),
        Input('suggest_top', 'value'),
        Input('what', 'value'),
        Input("dimred_nn", "value"),
        Input("dimred_md", "value"),
        Input("cluster_mcs", "value"),
        Input("cluster_ms", "value"),
    ]
)
def update_graph(
        fom_type, xdef, ydef, suggest_k, comp_cutoff, suggest_top, what, nn, md, mcs, ms
):
    if suggest_top == "+x":
        suggest_top = True
    else:
        suggest_top = False
    suggest_df, dimred_fp, dimred_fe, fp_labels, fe_labels = make_suggestions(
        fom_type, xdef, suggest_k, suggest_top, ydef, comp_cutoff, nn, md, mcs, ms,
    )
    if what == 'xy':
        x = suggest_df[xdef]
        y = suggest_df[ydef]
        color = 'rgba(135, 206, 250, 0.99)'
    elif what == 'dimred_FP':
        x = dimred_fp.T[0]
        y = dimred_fp.T[1]
        color = fp_labels
        xdef = "dimred_x"
        ydef = "dimred_y"
    elif what == 'dimred_FE':
        x = dimred_fe.T[0]
        y = dimred_fe.T[1]
        color = fe_labels
        xdef = "dimred_x"
        ydef = "dimred_y"

    fig = go.Figure(
        data=go.Scattergl(
            name="pool",
            x=x, y=y,
            mode='markers',
            customdata=suggest_df["smiles"],
            marker=dict(
                color=color,
                size=5,
                colorscale="Viridis"
            )
        )
    )

    fig.update_layout(
        dict(height=400,
             xaxis={'title': xdef},
             yaxis={'title': ydef},
             margin={'l': 40, 'b': 40, 't': 10, 'r': 10},
             legend={'x': 1, 'y': 1},
             hovermode=False,
             dragmode='select'
             )
    )
    return fig


TMP_selected_csv = f"{get_basename(__file__)}__selected.csv"


@app.callback(
    Output("download-dataframe-csv", "data"),
    [
        Input("btn_csv", "n_clicks"),
    ],
    prevent_initial_call=True,
)
def update_table_download(n_clicks):
    if file_exists(TMP_selected_csv):
        df = pd.read_csv(TMP_selected_csv)
    else:
        df = pd.DataFrame()
    return dcc.send_data_frame(df.to_csv, TMP_selected_csv, index=False)


@app.callback(
    Output('ligand_table', 'children'),
    [
        Input('graph', 'selectedData')
    ]
)
def update_table(selectedData):
    if selectedData:
        points = selectedData["points"]
        selected_records = []
        for point in points:
            record = {
                "smiles": point["customdata"],
                "structure": html.Img(src=get_img_str(point["customdata"])),
            }
            selected_records.append(record)
        selected_data_frame = pd.DataFrame.from_records(selected_records)
        removefile(TMP_selected_csv)
        selected_data_frame.to_csv(TMP_selected_csv, index=False)
        return [dbc.Table.from_dataframe(selected_data_frame, striped=True, bordered=True, hover=True, responsive="sm")]
    else:
        return []


if __name__ == "__main__":
    removefile(TMP_selected_csv)
    port = int("8{}".format(os.path.basename(__file__)[:3]))
    app.run_server(debug=False, host="0.0.0.0", port=port)