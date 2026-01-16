from __future__ import print_function
import os,sys,argparse

parser = argparse.ArgumentParser("test_3d lardly viewer")
parser.add_argument("-ll","--larlite",required=True,type=str,help="larlite file with dltagger_allreco tracks")
parser.add_argument("-e","--entry",required=True,type=int,help="Entry to load")
parser.add_argument("-ns","--no-timeshift",action="store_true",default=False,help="Do not apply time-shift")

args = parser.parse_args(sys.argv[1:])

import os
import json
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from larlite import larlite
from larcv import larcv
import lardly


input_larlite = args.larlite
ientry        = args.entry

# LARLITE
io_ll = larlite.storage_manager(larlite.storage_manager.kREAD)
io_ll.add_in_filename( input_larlite )
io_ll.open()

# Detector outline
detdata = lardly.DetectorOutline()
crtdet  = lardly.CRTOutline()


def load_event( io_ll, ientry ):

    entry_data = { "flash":[] }
    
    # Load entry
    io_ll.go_to(ientry)
    
    # opflash
    evopflash_beam   = io_ll.get_data(larlite.data.kOpFlash,"simpleFlashBeam")
    evopflash_cosmic = io_ll.get_data(larlite.data.kOpFlash,"simpleFlashCosmic")

    
    entry_data["flash"] += lardly.data.larlite_opflash_3d( evopflash_beam.at(0) )

    return entry_data

entry_data = load_event(io_ll,0)
#print(entry_data["flash"])

app = dash.Dash(
    __name__,
    meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
)

server = app.server

axis_template = {
    "showbackground": True,
    "backgroundcolor": "#141414",
    "gridcolor": "rgb(255, 255, 255)",
    "zerolinecolor": "rgb(255, 255, 255)",
}

plot_layout = {
    "title": "",
    "height":800,
    "margin": {"t": 0, "b": 0, "l": 0, "r": 0},
    "font": {"size": 12, "color": "white"},
    "showlegend": False,
    "plot_bgcolor": "#141414",
    "paper_bgcolor": "#141414",
    "scene": {
        "xaxis": axis_template,
        "yaxis": axis_template,
        "zaxis": axis_template,
        "aspectratio": {"x": 1, "y": 1, "z": 1},
        "camera": {"eye": {"x": 1, "y": 1, "z": 1},
                   "up":dict(x=0, y=1, z=0)},
        "annotations": [],
    },
}

testline = {
    "type":"scattergl",
    "x":[200,400,400,800],
    "y":[3200,3400,3800,4400],
    "mode":"markers",
    #"line":{"color":"rgb(255,255,255)","width":4},
    "marker":dict(size=10, symbol="triangle-up",color="rgb(255,255,255)"),
    }

app.layout = html.Div( [
    html.Div( [
        dcc.Graph(
            id="det3d",
            figure={
                "data": detdata.getlines()+crtdet.getlines()+entry_data["flash"],
                "layout": plot_layout,
            },
            config={"editable": True, "scrollZoom": False},
        )],
              className="graph__container"),
    ] )

if __name__ == "__main__":
    app.run_server(debug=True)