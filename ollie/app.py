# -*- coding: utf-8 -*-
import dash
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output

from tree_chart import tree_sql, tree_chart

tc = tree_chart(tree_sql)

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

app.layout = html.Div(children=[
    html.H1(children='BCX Insights'),
    html.Div(id='msgbox', children='''
        This is a test.
    '''),

    dcc.Graph(
        id='tree_chart', figure=tc
    ),

])


@app.callback(
    Output('tree_chart', 'figure'),
    [Input('tree_chart', 'clickData')])
def generate_tree(node_click):
    print(node_click)
    if node_click is not None:

        num_nodes = len(tc.data[1].marker.color)
        colours = tc.data[1].marker.color

        if colours[node_click['points'][0]['pointIndex']] != 'blue':

            colours = ['green'] * num_nodes
            alphas = [0.2] * num_nodes

            colours[node_click['points'][0]['pointIndex']] = 'blue'
            alphas[node_click['points'][0]['pointIndex']] = 1

            tc.data[1].marker.color = colours
            tc.data[1].marker.opacity = alphas

        else:

            tc.data[1].marker.color = ['green'] * num_nodes
            tc.data[1].marker.opacity = [1] * num_nodes
            tc.data = tc.data[0:2]

    return tc


if __name__ == '__main__':
    app.run_server(debug=True)
