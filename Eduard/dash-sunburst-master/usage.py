import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
from Sunburst_Queries import Overall_Query
from datetime import datetime as dt, timedelta
import pandas as pd
import numpy as np

from dash_sunburst import Sunburst

app = dash.Dash('')

app.scripts.config.serve_locally = True
app.css.config.serve_locally = True

Data_df = pd.read_gbq(Overall_Query,
                project_id = 'bcx-insights',
                dialect = 'standard')

Source_List = Data_df.source.value_counts().to_dict()

Source_List

for k in Source_List.keys():
    (k, Source_List[k])

sunburst_data = {
    'name': 'house',
    'children': [
        {
            'name': 'living room',
            'children': [
                {'name': 'Fixed', 'size': Source_List['F']},
                {'name': 'Mobile', 'size': Source_List['M']},
            ]
        },
        {
            'name': 'kitchen',
            'children': [
                {'name': 'Fixed', 'size': Source_List['F']},
                {'name': 'Mobile', 'size': Source_List['M']},
            ]
        },
    ]
}


app.layout = html.Div([
    html.H1('Telkom Customer Sunburst',style={'text-align':'center'}),
    html.Div([dcc.DatePickerRange(
    display_format='YYYY-MM-DD',
    start_date_placeholder_text='YYYY-MMM-DD',
    end_date_placeholder_text='YYYY-MMM-DD',
    max_date_allowed=dt.today().date(),
    end_date=dt.today().date(),
    start_date=(dt.today() - timedelta(days=30)).date()
    )], style = {'width': '100%', 'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}), 
    html.Div(
        [Sunburst(id='sun', data=sunburst_data)],
        style={'width': '100%', 'display': 'flex', 'align-items': 'center', 'justify-content': 'center', 'padding-top': '10px'}),
    html.Div(id='output', style={'clear': 'both','display': 'flex', 'align-items': 'center', 'justify-content': 'center', 'padding-top': '10px'})
])

app.config['suppress_callback_exceptions']=True

@app.callback(Output('output', 'children'), [Input('sun', 'selectedPath')])
def display_selected(selected_path):
    return 'You have selected path: {}'.format('->'.join(selected_path or []) or 'root')

@app.callback(Output('graph', 'figure'), [Input('sun', 'data'), Input('sun', 'selectedPath')])
def display_graph(data, selected_path):
    x = []
    y = []
    text = []
    color = []
    joined_selected = '->'.join(selected_path or [])

    SELECTED_COLOR = '#03c'
    SELECTED_CHILDREN_COLOR = '#8cf'
    SELECTED_PARENTS_COLOR = '#f80'
    DESELECTED_COLOR = '#ccc'

    def node_color(node_path):
        joined_node = '->'.join(node_path)
        if joined_node == joined_selected:
            return SELECTED_COLOR
        if joined_node.startswith(joined_selected):
            return SELECTED_CHILDREN_COLOR
        if joined_selected.startswith(joined_node):
            return SELECTED_PARENTS_COLOR
        return DESELECTED_COLOR

    def append_point(child_count, size, node, node_path):
        x.append(child_count)
        y.append(size)
        text.append(node['name'])
        color.append(node_color(node_path))

    def crawl(node, node_path):
        if 'size' in node:
            append_point(1, node['size'], node, node_path)
            return (1, node['size'])
        else:
            node_count, node_size = 1, 0
            for child in node['children']:
                this_count, this_size = crawl(child, node_path + [child['name']])
                node_count += this_count
                node_size += this_size
            append_point(node_count, node_size, node, node_path)
            return (node_count, node_size)

    crawl(data, [])

    layout = {
        'width': 1000,
        'height': 1000,
        'xaxis': {'title': 'Total Nodes', 'type': 'log'},
        'yaxis': {'title': 'Total Size', 'type': 'log'},
        'hovermode': 'closest'
    }

    return {
        'data': [{
            'x': x,
            'y': y,
            'text': text,
            'textposition': 'middle right',
            'marker': {
                'color': color,
                'size': [(v*v + 100)**0.5 for v in y],
                'opacity': 0.5
            },
            'mode': 'markers+text',
            'cliponaxis': False
        }],
        'layout': layout
    }

if __name__ == '__main__':
    app.run_server(debug=True)
