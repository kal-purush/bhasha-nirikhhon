import dash
import dash_html_components as html
import dash_core_components as dcc
import dash.dependencies as dd
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import plotly.graph_objs as go

import networkx as nx
import numpy as np
from io import BytesIO
from wordcloud import WordCloud
import base64
import pandas as pd
import pickle
import random

from app import app

### VARIABELS ###
name = "Mette-Frederiksen"



# data
bipartite_file = "data/bipartite_network_Mette-Frederiksen.p"
wordcloud_file = "data/adm_wordcould_Mette Frederiksen.p"
wordcloud_tfidf_file = "data/adm_wordcould_tfidf_Mette Frederiksen.p"


####### Bipartite
bipartite_data = pickle.load(open(bipartite_file, "rb"))
num_nodes = 474

fig = go.Figure(data=bipartite_data,
             layout=go.Layout(
                plot_bgcolor = "#f9f9f9",
                paper_bgcolor = "#f9f9f9",
                height= 600,
                width= 600,
                title='<br>Bipartite graph of the ' + name + ' government',
                #titlefont=dict(size=16),
                showlegend=False,
                hovermode='closest',
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))

bip_lars = html.Div([

    dbc.Row(dbc.Col(dcc.Graph(id='Graph_network',figure=fig),className="pretty_container")),
    dbc.Row(dbc.Col(html.P('test'),className="pretty_container"))

])
####### wordcload
wordcloud_data = pickle.load(open(wordcloud_file, "rb"))
wordcloud_tfidf_data = pickle.load(open(wordcloud_tfidf_file, "rb"))

wordcload = html.Div(dbc.Row([
                                dbc.Col([
                                    html.P("Having analysed the sentiment, it makes sense to also look at what words are being used when the questions are considerably negative or positive. Negative or positive are defined as questions with average sentiment lower or higher than 2 standard deviations from the mean."),
                                    dbc.RadioItems(
                                        id='radio-items',
                                        options = [
                                            {'label': 'Word frequency (TF)', 'value': 0},
                                            {'label': 'Important words (TF-IDF)', 'value': 1},
                                            ],
                                        value = 0,
                                        inline=False,
                                        style={"margin-bottom": "15px"}),
                                    dcc.Dropdown(id="community_dropdown", options=[{'label':'Community ' + str(i+1), 'value': i} for i in range(len(wordcloud_data))],
                                                                    multi=False, value=0, style={"margin-left": "auto","margin-right": "auto","width": "100%", "margin-bottom": "15px"}),
                                                                    html.P("Text.fjdfgjkhfgkhjdfgkjhdfgjhkdfjhkdjhkgfdfgjhkdfgkjhdfjkh")], width=4),
                                dbc.Col([html.H3("Wordcloud of Community"),html.Img(id='community_words')],style={'text-align':'center'}, width=4),
                                ]
                            ),className="pretty_container")

@app.callback(dd.Output('community_words', 'src'),
            [dd.Input('radio-items', 'value'),
            dd.Input('community_dropdown', 'value')])
def make_wordcloud(type, community):
    img_Community = BytesIO()
    if type == 0:
        data = wordcloud_data
    else:
        data = wordcloud_tfidf_data

    plot_wordcloud(data=data[community]).save(img_Community, format='PNG')
    return 'data:image/png;base64,{}'.format(base64.b64encode(img_Community.getvalue()).decode())

def plot_wordcloud(data):
    wc = WordCloud(background_color='#f9f9f9', width=410, height=390)
    wc.generate_from_frequencies(frequencies=data)

    def color_func(word, font_size, position, orientation, random_state=None,
                        **kwargs):

        h, s, l = (205, 98, 17)
        return "hsl(" + str(h) + "," + str(s) + "%," + str(random.randint(l-16, l)) + "%)"

    wc.recolor(color_func=color_func, random_state=3)
    return wc.to_image()









layout = html.Div(
    [
      bip_lars,
      wordcload
    ]

)