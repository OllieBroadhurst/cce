# -*- coding: utf-8 -*-
from datetime import datetime as dt, timedelta

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from tree_chart import get_figure, reset_fig
from tree_chart import find_journey, default_chart

from filters import service_options, customer_type
from filters import deal_desc, action_status

import json
from ast import literal_eval



external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']


graph = html.Div(dcc.Graph(id='tree_chart',
                    figure=default_chart()),
                    style={'float': 'left', 'width': '85%', 'height': '700px'}#,
                    )#className="nine columns")

filters = html.Div([
                    html.Div([html.H5(children='Service'),
                    dcc.Checklist(
                        id='service_filter',
                        children='Service Type',
                        options=service_options())],
                        style={'padding-bottom': '18px'}),

                    html.Div([html.H5(children='Actions Since'),
                    dcc.DatePickerSingle(
                        id='date_filter',
                        display_format='YYYY-MM-DD',
                        #month_format='YYYY MMM DD',
                        placeholder='YYYY-MMM-DD',
                        date=dt.today() - timedelta(days=60))],
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
                        style={'padding-bottom': '18px'}),

                    html.Div([html.H5(children='Final Action Status'),
                    dcc.Dropdown(
                        id='action_status_filter',
                        children='Final Action Status',
                        multi=True,
                        options=action_status())],
                        style={'padding-bottom': '18px'})
                        ])

button = html.Div(html.Button('Show', id='run_button',
            style={'padding-bottom': '20px'}))

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


app.layout = html.Div([
    html.H1(children='BCX Insights'),
    html.Div([
        html.Div([button, filters], style={'width': '15%', 'float': 'left'}),
    graph]),
    html.Div(children='{}', id='history', style={'display': 'none'}),
    html.Div(children='{}', id='links', style={'display': 'none'}),
    html.Div(children='{}', id='times', style={'display': 'none'}),
    html.Div(children='{}', id='desc_map', style={'display': 'none'})]
    )

outputs = [Output('tree_chart', 'figure'), Output('history', 'children'),
            Output('links', 'children'), Output('times', 'children')]

inputs = [Input('run_button', 'n_clicks'), Input('tree_chart', 'clickData')]

states = [State('service_filter', 'value'), State('customer_type_filter', 'value'),
        State('history', 'children'), State('tree_chart', 'figure'),
        State('links', 'children'), State('times', 'children'),
        State('deal_desc_filter', 'value'), State('action_status_filter', 'value'),
        State('date_filter', 'date')]


@app.callback(outputs, inputs, states)
def generate_tree(click_btn, node_click, services, types,
                btn_history, current_fig, path_meta, durations,
                deals, status, date_val):
    history = json.loads(btn_history)
    paths = json.loads(path_meta)
    durations = json.loads(durations)

    path_dict = {literal_eval(k): v for k, v in paths.items()}
    durations = {literal_eval(k): literal_eval(v) for k, v in durations.items()}

    if deals is not None:
        deals = sum([literal_eval(d) for d in deals], [])


    if str(click_btn) not in history.keys() and click_btn is not None:
        fig, links, durations = get_figure(None, services, types, deals, status, date_val)

        history[click_btn] = 1
        paths = {str(k): v for k, v in links.items()}
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

        selection_x = node_click['points'][0]['x']
        selection_y = node_click['points'][0]['y']

        if colours[index] != 'blue':
            current_fig = find_journey(current_fig,
                            path_dict,
                            durations,
                            selection_x,
                            selection_y)

        durations = {str(k):str(v) for k, v in durations.items()}
        return current_fig, json.dumps(history), json.dumps(paths), json.dumps(durations)
    else:
        raise PreventUpdate


if __name__ == '__main__':
    app.run_server(debug=False)
