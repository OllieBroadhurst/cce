# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from tree_chart import get_figure, reset_fig, format_selection, find_journey, default_chart
from filters import service_options

import json
import webbrowser


IP = r'http://127.0.0.1:8050/'

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

check_list = dcc.Checklist(
                    id='service_filter',
                    children='Service Type',
                    options=service_options(),
                    style={'width': '20%',
                    'height':'20%',
                    'display': 'inline-block'}
                    )

graph = dcc.Graph(id='tree_chart',
                style={'width': '70%',
                'height':'50%',
                'display': 'inline-block',
                'figure': default_chart()})

app.layout = html.Div(children=[
    html.H1(children='BCX Insights'),
    html.Div(html.Button('Show', id='run_button')),
    html.Div(children='{}', id='history', style={'display': 'none'}),
    html.Div(children='{"0":{"(0,0)":"[(0,0)]"}}', id='links', style={'display': 'none'}),
    html.Div([
            html.Div(check_list),
            html.Div(graph)
            ])
    ])


@app.callback(
    [Output('tree_chart', 'figure'),
    Output('history', 'children'),
    Output('links', 'children')],
    [Input('run_button', 'n_clicks'),
    Input('tree_chart', 'clickData')],
    [State('service_filter', 'value'),
    State('history', 'children'),
    State('tree_chart', 'figure'),
    State('links', 'children')])
def generate_tree(click_btn, node_click, services, btn_history, current_fig, path_meta):
    history = json.loads(btn_history)
    paths = json.loads(path_meta)
    #path_dict = {tuple(k): list(v) for k, v in paths['0'].items()}

    if str(click_btn) not in history.keys() and click_btn is not None:
        fig, links = get_figure(services)

        history[click_btn] = 1
        paths = {str(k):str(v) for k, v in links.items()}
        return fig, json.dumps(history), json.dumps(paths)

    elif node_click is not None and history.get('1') is not None:
        colours = current_fig['data'][1]['marker']['color']
        num_nodes = len(colours)
        current_fig = reset_fig(current_fig)
        selection = node_click['points'][0]
        print(paths)
        if colours[selection['pointIndex']] != 'blue':
            current_fig = format_selection(current_fig, selection)
            current_fig = find_journey(current_fig,
                            paths,
                            node_click['points'][0]['x'],
                            node_click['points'][0]['y'])

        return current_fig, json.dumps(history), json.dumps(paths)
    else:
        raise PreventUpdate


if __name__ == '__main__':
    app.run_server(debug=True)
