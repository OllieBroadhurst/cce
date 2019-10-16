# -*- coding: utf-8 -*-
from datetime import datetime as dt, timedelta

import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from tree_chart import get_figure, find_journey, default_chart

from filters import service_options, customer_type, has_dispute, has_fault
from filters import deal_desc, action_status, action_type

import json
from ast import literal_eval



external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']
header_style = {'font-size': 12}
filter_style = {'padding-bottom': '18px', 'font-size': 12}

graph = html.Div(dcc.Graph(id='tree_chart',
                    figure=default_chart()),
                    style={'float': 'left', 'width': '82%', 'height': '600px'}#,
                    )

filters = html.Div([
                    html.Div([html.H5(children='Service', style=header_style),
                    dcc.Checklist(
                        id='service_filter',
                        children='Service Type',
                        options=service_options())],
                        style=filter_style),

                    html.Div([html.H5(children='Actions Since', style=header_style),
                    dcc.DatePickerSingle(
                        id='date_filter',
                        display_format='YYYY-MM-DD',
                        placeholder='YYYY-MMM-DD',
                        date=(dt.today() - timedelta(days=60)).date())],
                        style=filter_style),

                    html.Div([html.H5(children='Customer Type', style=header_style),
                    dcc.Dropdown(
                        id='customer_type_filter',
                        children='Customer Type',
                        multi=True,
                        options=customer_type())],
                        style=filter_style),

                    html.Div([html.H5(children='Deal Description', style=header_style),
                    dcc.Dropdown(
                        id='deal_desc_filter',
                        children='Deal Description',
                        multi=True,
                        options=deal_desc())],
                        style=filter_style),

                    html.Div([html.H5(children='Final Action Status', style=header_style),
                    dcc.Dropdown(
                        id='action_status_filter',
                        children='Final Action Status',
                        multi=True,
                        options=action_status())],
                        style=filter_style),

                    html.Div([html.H5(children='Final Action', style=header_style),
                    dcc.Dropdown(
                        id='final_action_filter',
                        children='Final Action Type',
                        multi=True,
                        options=action_type())],
                        style=filter_style),

                    html.Div([html.H5(children='Has Dispute', style=header_style),
                    dcc.Dropdown(
                        id='dispute_filter',
                        children='Has Dispute',
                        multi=False,
                        value='Either',
                        options=has_dispute())],
                        style=filter_style),

                    html.Div([html.H5(children='Has Fault', style=header_style),
                    dcc.Dropdown(
                        id='fault_filter',
                        children='Has Fault',
                        multi=False,
                        value='Either',
                        options=has_fault())],
                        style=filter_style)
                        ], style={'overflow-y':'scroll', 'height':'620px'})

button = html.Div(html.Button('Show', id='run_button',
            style={'padding-bottom': '20px'}))

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


app.layout = html.Div([
    html.H1(children='BCX Insights'),

            html.Div([
                html.Div([button, filters], style={'width': '17%', 'float': 'left'}),
                graph]),
            html.Div(children='{}', id='history', style={'display': 'none'}),
            html.Div(children='{}', id='links', style={'display': 'none'}),
            html.Div(children='{}', id='routes', style={'display': 'none'}),
            html.Div(children='{}', id='desc_map', style={'display': 'none'})
            ]
    )

outputs = [Output('tree_chart', 'figure'), Output('history', 'children'),
            Output('links', 'children'), Output('routes', 'children')]

inputs = [Input('run_button', 'n_clicks'), Input('tree_chart', 'clickData')]

states = [State('service_filter', 'value'), State('customer_type_filter', 'value'),
        State('history', 'children'), State('tree_chart', 'figure'),
        State('links', 'children'), State('routes', 'children'),
        State('deal_desc_filter', 'value'), State('action_status_filter', 'value'),
        State('date_filter', 'date'), State('dispute_filter', 'value'),
        State('final_action_filter', 'value'), State('fault_filter', 'value')]


@app.callback(outputs, inputs, states)
def generate_tree(click_btn, node_click, services, types,
                btn_history, current_fig, links, routes,
                deals, status, date_val, dispute_val,
                action_filter, fault_filter):
    history = json.loads(btn_history)
    links = json.loads(links)
    routes = json.loads(routes)

    links = {literal_eval(k): v for k, v in links.items()}
    routes = {literal_eval(k): literal_eval(v) for k, v in routes.items()}

    if deals is not None:
        deals = sum([literal_eval(d) for d in deals], [])

    if str(click_btn) not in history.keys() and click_btn is not None:
        fig, links, routes = get_figure(None, services, types, deals, status,
                                        date_val, dispute_val, action_filter,
                                        fault_filter)

        history[click_btn] = 1
        links = {str(k): v for k, v in links.items()}
        routes = {str(k):str(v) for k, v in routes.items()}

        return fig, json.dumps(history), json.dumps(links), json.dumps(routes)

    elif node_click is not None and history.get('1') is not None:

        selection_x = node_click['points'][0]['x']
        selection_y = node_click['points'][0]['y']

        current_fig = find_journey(current_fig,
                        links,
                        routes,
                        selection_x,
                        selection_y)

        links = {str(k): v for k, v in links.items()}
        routes = {str(k):str(v) for k, v in routes.items()}
        return current_fig, json.dumps(history), json.dumps(links), json.dumps(routes)
    else:
        raise PreventUpdate


if __name__ == '__main__':
    app.run_server(debug=True)
