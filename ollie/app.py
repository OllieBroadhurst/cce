# -*- coding: utf-8 -*-
from datetime import datetime as dt, timedelta

import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from tree_chart import get_figure, find_journey, default_chart

from filters import service_options, customer_type, has_dispute, has_fault
from filters import deal_desc, action_status, action_type

import json
from ast import literal_eval


header_style = {'font-size': '16px'}
filter_padding_top = '20px'
filter_float = 'right'
filter_height = '48px'
filter_padding_right = '30px'
filter_font_size = '12px'

graph = html.Div(dcc.Graph(id='tree_chart',
                    figure=default_chart(), style={'overflow-y': 'hidden'}),
                    style={'padding-top': '5px', 'padding-right': '5px',
                        'float': 'right', 'width': '100%'}
                    )


top_filters = html.Div([
                    html.Div([html.H5(children='Service', style=header_style),
                    dcc.Checklist(
                        id='service_filter',
                        children='Service Type',
                        options=service_options(),
                        inputStyle={'margin-left': '10px'},
                        style={'height': filter_height})],
                        style={'padding-top': filter_padding_top,
                                'float': filter_float,
                                'padding-right': filter_padding_right}),

                    html.Div([html.H5(children='Final Action', style=header_style),
                    dcc.Dropdown(
                        id='final_action_filter',
                        children='Final Action Type',
                        multi=True,
                        options=action_type(),
                        style={'height': filter_height, 'width': '200px'})],
                        style={'padding-top': filter_padding_top,
                                'float': filter_float,
                                'padding-right': filter_padding_right}),

                    html.Div([html.H5(children='Customer Type', style=header_style),
                    dcc.Dropdown(
                        id='customer_type_filter',
                        children='Customer Type',
                        multi=True,
                        options=customer_type(),
                        style={'width': '200px', 'height': filter_height})],
                        style={'padding-top': filter_padding_top,
                                'float': filter_float,
                                'padding-right': filter_padding_right}),

                    html.Div([html.H5(children='Final Action Status', style=header_style),
                    dcc.Dropdown(
                        id='action_status_filter',
                        children='Final Action Status',
                        multi=True,
                        options=action_status(),
                        style={'height': filter_height, 'width': '170px'})],
                        style={'padding-top': filter_padding_top,
                                'float': filter_float,
                                'padding-right': filter_padding_right}),

                    html.Div([html.H5(children='Minimum journey hours: 0', style=header_style, id='hours_header'),
                    dcc.Slider(
                            id='hours_slider',
                            min=0,
                            max=2000,
                            step=1,
                            value=0
                        )],
                        style={'padding-top': filter_padding_top,
                                'float': filter_float,
                                'padding-right': filter_padding_right,
                                'width': '250px'})])

bottom_filters = html.Div([
                    html.Div([html.H5(children='Date Range', style=header_style),
                    dcc.DatePickerRange(
                        id='date_filter',
                        display_format='YYYY-MM-DD',
                        start_date_placeholder_text='YYYY-MMM-DD',
                        end_date_placeholder_text='YYYY-MMM-DD',
                        max_date_allowed=dt.today().date(),
                        end_date=dt.today().date(),
                        start_date=(dt.today() - timedelta(days=60)).date(),
                        style={'height': filter_height}
                        )],
                        style={'padding-top': filter_padding_top,
                                'float': filter_float,
                                'padding-right': filter_padding_right}),

                    html.Div([html.H5(children='Deal Description', style=header_style),
                    dcc.Dropdown(
                        id='deal_desc_filter',
                        children='Deal Description',
                        multi=True,
                        options=deal_desc(),
                        style={'height': filter_height, 'width': '250px'})],
                        style={'padding-top': filter_padding_top,
                                'float': filter_float,
                                'padding-right': filter_padding_right}),

                    html.Div([html.H5(children='Has Dispute', style=header_style),
                    dcc.Dropdown(
                        id='dispute_filter',
                        children='Has Dispute',
                        multi=False,
                        value='Either',
                        options=has_dispute(),
                        style={'height': filter_height, 'width': '100px'})],
                        style={'padding-top': filter_padding_top,
                                'float': filter_float,
                                'padding-right': filter_padding_right}),

                    html.Div([html.H5(children='Has Fault', style=header_style),
                    dcc.Dropdown(
                        id='fault_filter',
                        children='Has Fault',
                        multi=False,
                        value='Either',
                        options=has_fault(),
                        style={'height': filter_height, 'width': '100px'})],
                        style={'padding-top': filter_padding_top,
                                'float': filter_float,
                                'padding-right': filter_padding_right})], style={'padding-top': '2px'})

button = html.Button('Run', id='run_button', style={'float': 'right'})

collapse_button = html.Button(className='mr-1',
                    children=html.Span(
                                html.I(className='fas fa-angle-up',
                                        id='button-icon',
                                        style={'aria-hidden': 'true'})),
                    id='collapse-button', style={'float': 'right'})

filter_panel_style = {'float': 'right', 'width': '1300px', 'height': '250px'}
filter_panel =  dbc.Collapse(children=dbc.Card([button, top_filters, bottom_filters], style=filter_panel_style),
                                    id='collapse2',
                                    is_open=True)

FONT_AWESOME = "https://use.fontawesome.com/releases/v5.7.2/css/all.css"

app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, FONT_AWESOME])


app.layout = html.Div([
            html.Div([collapse_button, filter_panel], style={'padding-top': '5px'}),
            graph,
            html.Div(children='{}', id='history', style={'display': 'none'}),
            html.Div(children='{}', id='links', style={'display': 'none'}),
            html.Div(children='{}', id='routes', style={'display': 'none'}),
            html.Div(children='{}', id='desc_map', style={'display': 'none'})
            ],
            style={'overflow-y': 'hidden'})

outputs = [Output('tree_chart', 'figure'), Output('history', 'children'),
            Output('links', 'children'), Output('routes', 'children')]

inputs = [Input('run_button', 'n_clicks'), Input('tree_chart', 'clickData')]

states = [State('service_filter', 'value'), State('customer_type_filter', 'value'),
        State('history', 'children'), State('tree_chart', 'figure'),
        State('links', 'children'), State('routes', 'children'),
        State('deal_desc_filter', 'value'), State('action_status_filter', 'value'),
        State('date_filter', 'start_date'), State('date_filter', 'end_date'),
        State('dispute_filter', 'value'), State('final_action_filter', 'value'),
        State('fault_filter', 'value'), State('hours_slider', 'value')]


@app.callback(outputs, inputs, states)
def generate_tree(click_btn, node_click, services, types,
                btn_history, current_fig, links, routes,
                deals, status, start_date_val, end_date_val, dispute_val,
                action_filter, fault_filter, min_hours):
    history = json.loads(btn_history)
    links = json.loads(links)
    routes = json.loads(routes)

    links = {literal_eval(k): v for k, v in links.items()}
    routes = {literal_eval(k): literal_eval(v) for k, v in routes.items()}

    if deals is not None:
        deals = sum([literal_eval(d) for d in deals], [])

    if str(click_btn) not in history.keys() and click_btn is not None:
        fig, links, routes = get_figure(None, services, types, deals, status,
                                        start_date_val, end_date_val, dispute_val,
                                        action_filter, fault_filter, min_hours)

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


@app.callback(
    [Output("collapse2", "is_open"),
    Output("button-icon", "className")],
    [Input("collapse-button", "n_clicks")],
    [State("collapse2", "is_open")],)
def toggle_collapse(n, is_open):
    if n is not None:
        if is_open:
            return not is_open, 'fas fa-angle-down'
        else:
            return not is_open, 'fas fa-angle-up'
    raise PreventUpdate


@app.callback(
    Output('hours_header', 'children'),
    [Input('hours_slider', 'value')])
def update_output(value):
    return f'Minimum journey hours: {value}'


if __name__ == '__main__':
    app.run_server(debug=True)
