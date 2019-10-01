# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from tree_chart import get_figure, reset_fig, format_selection, find_journey, default_chart
from filters import service_options, customer_type

import json
from ast import literal_eval



external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']


graph = html.Div(dcc.Graph(id='tree_chart',
                    figure=default_chart()),
                    style={'float': 'right', 'padding-right': '15px'},
                    className="ten columns")

filters = html.Div([
                    html.Div([html.H5(children='Service'),
                    dcc.Checklist(
                        id='service_filter',
                        children='Service Type',
                        options=service_options())]),

                    html.Div([html.H5(children='Customer Type'),
                    dcc.Dropdown(
                        id='customer_type_filter',
                        children='Customer Type',
                        multi=True,
                        options=customer_type())])],
                        style={'font-size': '12px'},
                    #'height':'20%'}
                    className="two columns")

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.H1(children='BCX Insights'),
    html.Div(html.Button('Show', id='run_button'),
    style={'padding-bottom': '10px'}),
    html.Div(children='{}', id='history', style={'display': 'none'}),
    html.Div(children='{}', id='links', style={'display': 'none'}),
    html.Div(children='{}', id='times', style={'display': 'none'}),
    html.Div([
        filters,
        graph], className="row")])


@app.callback(
    [Output('tree_chart', 'figure'),
    Output('history', 'children'),
    Output('links', 'children')],
    [Input('run_button', 'n_clicks'),
    Input('tree_chart', 'clickData')],
    [State('service_filter', 'value'),
    State('customer_type_filter', 'value'),
    State('history', 'children'),
    State('tree_chart', 'figure'),
    State('links', 'children'),
    State('times', 'children')])
def generate_tree(click_btn, node_click, services, types,
                btn_history, current_fig, path_meta, durations):
    history = json.loads(btn_history)
    paths = json.loads(path_meta)

    path_dict = {literal_eval(k): literal_eval(v) for k, v in paths.items()}

    if str(click_btn) not in history.keys() and click_btn is not None:
        fig, links, durations = get_figure(services, types)

        history[click_btn] = 1
        paths = {str(k):str(v) for k, v in links.items()}
        return fig, json.dumps(history), json.dumps(paths)

    elif node_click is not None and history.get('1') is not None:
        colours = current_fig['data'][1]['marker']['color']
        num_nodes = len(colours)
        current_fig = reset_fig(current_fig)
        selection = node_click['points'][0]

        index = selection['pointIndex']

        if index > len(colours) - 1:
            index = len(colours) - 1

        if colours[index] != 'blue':
            current_fig = format_selection(current_fig, selection)
            current_fig = find_journey(current_fig,
                            path_dict,
                            durations,
                            node_click['points'][0]['x'],
                            node_click['points'][0]['y'])

        return current_fig, json.dumps(history), json.dumps(paths)
    else:
        raise PreventUpdate


if __name__ == '__main__':
    app.run_server(debug=True)
