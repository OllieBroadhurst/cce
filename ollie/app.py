# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from tree_chart import get_figure, reset_fig, format_selection, find_journey, default_chart
from filters import service_options, customer_type, deal_desc

import json
from ast import literal_eval



external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']


graph = html.Div(dcc.Graph(id='tree_chart',
                    figure=default_chart()),
                    style={'padding-right': '15px', 'display': 'inline-block', 'height': '800px'},
                    className="ten columns")

filters = html.Div([
                    html.Div([html.H5(children='Service'),
                    dcc.Checklist(
                        id='service_filter',
                        children='Service Type',
                        options=service_options())],
                        style={'padding-bottom': '18px'}),

                    html.Div([html.H5(children='Customer Type'),
                    dcc.Dropdown(
                        id='customer_type_filter',
                        children='Customer Type',
                        multi=True,
                        options=customer_type())],
                        style={'padding-bottom': '18px'}),

                    html.Div([html.H5(children='Deal Description'),
                    dcc.Dropdown(
                        id='deal_desc_filter',
                        children='Deal Description',
                        multi=True,
                        options=deal_desc())],
                        style={'padding-bottom': '18px'})
                        ],
                        style={'font-size': '12px'},
                    #'height':'20%'}
                    className="two columns")

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div([
    html.H1(children='BCX Insights'),
    html.Div(html.Button('Show', id='run_button'),
    style={'padding-bottom': '20px'}),
    html.Div(children='{}', id='history', style={'display': 'none'}),
    html.Div(children='{}', id='links', style={'display': 'none'}),
    html.Div(children='{}', id='times', style={'display': 'none'}),
    html.Div(children='{}', id='desc_map', style={'display': 'none'}),
    html.Div([filters, graph], style={'height': '800px'})])


outputs = [Output('tree_chart', 'figure'), Output('history', 'children'),
            Output('links', 'children'), Output('times', 'children')]

inputs = [Input('run_button', 'n_clicks'), Input('tree_chart', 'clickData')]

states = [State('service_filter', 'value'), State('customer_type_filter', 'value'),
        State('history', 'children'), State('tree_chart', 'figure'),
        State('links', 'children'), State('times', 'children'),
        State('deal_desc_filter', 'value')]


@app.callback(outputs, inputs, states)
def generate_tree(click_btn, node_click, services, types,
                btn_history, current_fig, path_meta, durations,
                deals):
    history = json.loads(btn_history)
    paths = json.loads(path_meta)
    durations = json.loads(durations)


    path_dict = {literal_eval(k): literal_eval(v) for k, v in paths.items()}
    durations = {literal_eval(k): literal_eval(v) for k, v in durations.items()}

    if deals is not None:
        deals = sum([literal_eval(d) for d in deals], [])


    if str(click_btn) not in history.keys() and click_btn is not None:
        fig, links, durations = get_figure(services, types, deals)

        history[click_btn] = 1
        paths = {str(k):str(v) for k, v in links.items()}
        durations = {str(k):str(v) for k, v in durations.items()}

        return fig, json.dumps(history), json.dumps(paths), json.dumps(durations)

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

        durations = {str(k):str(v) for k, v in durations.items()}
        return current_fig, json.dumps(history), json.dumps(paths), json.dumps(durations)
    else:
        raise PreventUpdate


if __name__ == '__main__':
    app.run_server(debug=True)
