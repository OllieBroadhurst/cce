# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate

from tree_chart import tree_chart
from filters import service_options

import json

IP = r'http://127.0.0.1:8050/'

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

tc = tree_chart()
fig = tc.get_figure()

app.layout = html.Div(children=[
    html.H1(children='BCX Insights'),
    html.Div(html.Button('Show', id='run_button')),
    html.Div('{}', id='history', style={'display': 'none'}),
    html.Div(
                dcc.Checklist(
                    id='service_filter',
                    children='Service Type',
                    options=service_options(),
                    style={'width': '20%',
                    'height':'20%',
                    'display': 'inline-block'}
                    )),
    html.Div(
                dcc.Graph(id='tree_chart',
                    #figure=tree_chart().get_figure(),
                    style={'width': '70%',
                    'height':'50%',
                    'display': 'inline-block'})
                )])



@app.callback(
    [Output('tree_chart', 'figure'),
    Output('history', 'children')],
    [Input('run_button', 'n_clicks'),
    Input('tree_chart', 'clickData')],
    [State('service_filter', 'value'),
    State('history', 'children')])
def generate_tree(click_btn, node_click, services, btn_history):
    history = json.loads(btn_history)

    if str(click_btn) not in history.keys():
        if click_btn is not None and click_btn > 0:
            tc = tree_chart(services)
            fig = tc.get_figure()
            history[click_btn] = 1

            return fig, json.dumps(history)

    print(node_click)
    if node_click is not None and click_btn is not None:
        #tc = tree_chart()
        num_nodes = tc.num_nodes
        colours = tc.colours
        tc.reset_fig()
        selection = node_click['points'][0]
        if colours[selection['pointIndex']] != 'blue':
            tc.format_selection(selection)
            tc.find_journey(node_click['points'][0]['x'],
                            node_click['points'][0]['y'])

    else:
        tc = tree_chart(services)
        fig = tc.get_figure()

    return fig, json.dumps(history)

if __name__ == '__main__':
    app.run_server(debug=True)
