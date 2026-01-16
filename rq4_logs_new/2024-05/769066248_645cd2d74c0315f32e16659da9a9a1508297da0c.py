import pandas as pd
from dash import Input, Output, callback, Dash, html, dcc, State
import dash_cytoscape as cyto
import json

def feature_table(data,from_att_df,to_att_df,grant_to_people_df,iterator):
    
        ID=int(data[iterator]["id"])
        if ID in from_att_df['Node'].values:
            row_value = from_att_df[from_att_df['Node'] == ID].iloc[0]
            df = pd.DataFrame(columns=['Selected Node', str(ID)])
            for column_name, value in list(row_value.items())[2:]:
                if column_name !='Node_Size':
                    column_name = truncate_with_dots(column_name)
                    df.loc[len(df)] = [column_name, value]
            filtered_df = grant_to_people_df[grant_to_people_df.eq(ID).any(axis=1)].iloc[:, 1]
            if len(filtered_df) % 2 != 0:
                filtered_df=pd.concat([filtered_df, pd.Series([' '])])
            num_rows = (len(filtered_df) + 1) // 2
            reshaped_df = pd.DataFrame(filtered_df.values.reshape(num_rows, 2))    
            
        elif ID in to_att_df['Node'].values:
            row_value = to_att_df[to_att_df['Node'] == ID].iloc[0]
            df = pd.DataFrame(columns=['Selected Node', str(ID)])
            for column_name, value in list(row_value.items())[2:]:
                if column_name !='Node_Size':
                    column_name = truncate_with_dots(column_name)
                    df.loc[len(df)] = [column_name, value]
            filtered_df = grant_to_people_df[grant_to_people_df.eq(ID).any(axis=1)].iloc[:, 0]
            if len(filtered_df) % 2 != 0:
                filtered_df=pd.concat([filtered_df, pd.Series([' '])])
            num_rows = (len(filtered_df) + 1) // 2
            reshaped_df = pd.DataFrame(filtered_df.values.reshape(num_rows, 2))
    
        reshaped_df.columns = ['Connected Nodes','']  
        return html.Div([html.Table(
            # Header
            [html.Tr([html.Th(col) for col in df.columns])] +
            # Body
            [html.Tr([html.Td(row[col]) for col in df.columns]) for idx, row in df.iterrows()],
            style={
                'borderCollapse': 'collapse',
                'border': '1px solid black',
                'fontFamily': "alegreya sans, sans-serif",
                'fontSize': '16px',
                'textAlign': 'center',
                'marginRight': '10px',
            'width': '100%',
            'table-layout': 'fixed',
            'justify-content': 'space-between'}
        ),html.Table(
            # Header
            [html.Tr([html.Th(col) for col in reshaped_df.columns])] +
            # Body
            [html.Tr([html.Td(row[col]) for col in reshaped_df.columns]) for idx, row in reshaped_df.iterrows()],
            style={
                'borderCollapse': 'collapse',
                'border': '1px solid black',
                'fontFamily': "alegreya sans, sans-serif",
                'fontSize': '16px',
                'textAlign': 'center',
                'marginRight': '10px'
            , 'marginBottom': '10px', 
            'background-color': '#dfe0e1',  
            'width': '100%',
            'table-layout': 'fixed',
            'justify-content': 'space-between'}
        )])
        
def truncate_with_dots(string):
    if len(str(string)) > 20:
        return string[:18] + ".."
    else:
        return string