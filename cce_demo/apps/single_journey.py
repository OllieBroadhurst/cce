import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import time
from pandas_gbq.gbq import GenericGBQException
import json
import dash_cytoscape as cyto
import pandas as pd
import plotly.graph_objects as go

from app import app

Overall_Query = """
SELECT *

FROM `bcx-insights.telkom_customerexperience.orders_20190926_00_anon` 

limit 5000
"""

Data_df = pd.read_gbq(Overall_Query,
                project_id = 'bcx-insights',
                dialect = 'standard')

label = ['Orders']
parent = ['']
value = [len(Data_df)]

for s in Data_df['SOURCE'].unique():
  label.append(s)
  parent.append('Orders')
  value.append(len(Data_df[Data_df['SOURCE']==s]))

  temp_df = Data_df[Data_df['SOURCE']==s]
  for osc in temp_df['ORIGINAL_SALES_CHANNEL_DESC'].unique():
    label.append(osc)
    parent.append(s)
    value.append(len(temp_df[temp_df['ORIGINAL_SALES_CHANNEL_DESC']==osc]))
    
fig =go.Figure(go.Sunburst(
    ids = label,
    labels=label,
    parents=parent,
    values=value,
    branchvalues="total",
    maxdepth=2,
    #insidetextfont = {'size':15},
    hoverinfo = 'all',
    
))

fig.update_layout(margin = dict(t=0, l=0, r=0, b=0))




elements = [{'data': {'id': 'gen-node', 'name': "Search for an Account Number...", 
                      'weight': 65, 
                      'faveColor': '#095575',
                      'faveShape': 'ellipse',
                      'text-size':3,
                      #'position': {'x': 800, 'y': 367}, 
                      #'selectable': False,
                      }}]



cyto_journey = html.Div([
    cyto.Cytoscape(
        id='cytoscape',
        elements=elements,
        zoomingEnabled = True, 
        zoom = 0.5,
        layout={
            'name': 'breadthfirst',
            'padding': 10
        },
        stylesheet=[{
            'selector': 'node',
            'style': {
                'shape': 'data(faveShape)',
                'width': 'mapData(weight, 40, 80, 20, 60)',
                'content': 'data(name)',
                'text-valign': 'center',
                'text-outline-width': 2,
                'text-outline-color': 'data(faveColor)',
                'background-color': 'data(faveColor)',
                'color': '#fff'
            }
        }, {
            'selector': 'edge',
            'style': {
                'curve-style': 'bezier',
                'opacity': 0.666,
                'width': 'mapData(strength, 70, 100, 2, 6)',
                'target-arrow-shape': 'triangle',
                'source-arrow-shape': 'circle',
                'line-color': 'data(faveColor)',
                'source-arrow-color': 'data(faveColor)',
                'target-arrow-color': 'data(faveColor)'
            }
        }, {
            'selector': ':selected',
            'style': {
                'border-width': 3,
                'border-color': '#333'
            }
        }, {
            'selector': 'edge.questionable',
            'style': {
                'line-style': 'dotted',
                'target-arrow-shape': 'diamond'
            }
        }, {
            'selector': '.faded',
            'style': {
                'opacity': 0.25,
                'text-opacity': 0
            }
        }],
        style={
            'width': '100%',
            'height': '93%',
            'position': 'absolute',
            'left': 0,
            'bottom': 0
        }
    )
])






def update_elements(acc_no,clicks, elements):
    
    
    orders = pd.io.gbq.read_gbq('''

    SELECT *
    FROM `bcx-insights.telkom_customerexperience.orders_20190926_00_anon`
    WHERE ACCOUNT_NO_ANON = {}
    LIMIT 10000

    '''.format(acc_no), project_id='bcx-insights', dialect='standard')


    orders = orders.sort_values(['ACCOUNT_NO_ANON', 'MSISDN_ANON', 'ORDER_ID_ANON', 'ORDER_CREATION_DATE'],ascending=True)

    udevices = orders['MSISDN_ANON'].unique()
    uactions = orders['ACTION_TYPE_DESC'].unique()

  
    nodes = [{'data': {'id': 'acc_no_anon_{}'.format(acc_no), 'name':'acc_no_anon_{}'.format(acc_no) , 'weight': 65, 'faveColor': '#550b78',
              'faveShape': 'ellipse'}}]
    edges = []


    # Create Device Nodes

    for device in udevices[:6]:
        id = 'device_' + str(device)
        node = {'data': {'id': id, 'name': id, 'weight': 65, 'faveColor': '#095575',
                'faveShape': 'ellipse'}}
        nodes.append(node)

    # Create Device Edges

    for device in udevices[:6]:
        id = 'device_' + str(device)
        edge = {'data': {'source': 'acc_no_anon_{}'.format(acc_no), 'target': id, 'faveColor': '#60a1bf',
                'strength': 60}}
        edges.append(edge)


    for device in udevices[:6]:
        df = orders[orders['MSISDN_ANON'] == device].sort_values('ORDER_CREATION_DATE')
        # Deals
        for deal in df['DEAL_DESC'].unique():
            id = str(device) +'_'+ str(deal)
            node = {'data': {'id': id, 'name': deal, 'weight': 65, 'faveColor': '#095575',
              'faveShape': 'ellipse'}}
            nodes.append(node)
            cn = node
            #print(cn['data']['name'])

            edge = {'data': {'source': 'device_' + str(device), 'target': id, 'faveColor': '#60a1bf',
                    'strength': 60}}
            edges.append(edge)
        # Actions
        #print(deal)
        #print('>>>')
        #print(df['ACTION_TYPE_DESC'].unique())
        #print()
            df2 = df[df['DEAL_DESC'] == deal]

            for action in df2['ACTION_TYPE_DESC'].unique():
                node = {'data': {'id': str(device) +'_'+ str(deal) +'_'+ str(action), 'name': action, 'weight': 65, 'faveColor': '#045c16',
                    'faveShape': 'ellipse'}}
                nodes.append(node)
      
                edge = {'data': {'source': cn['data']['id'], 'target': str(device) +'_'+ str(deal) +'_'+ str(action), 'faveColor': '#045c16',
                    'strength': 60}}
                edges.append(edge)

                cn = node
    elements = nodes + edges
    return elements




search_bar = dbc.Row(
    [
        dbc.Col(dbc.Input(type="search", placeholder="Search",id='cyto-search')),
        dbc.Col(
            dbc.Button("Search", color="primary", className="ml-2",id='cyto-search-button', n_clicks_timestamp=0),
            width="auto"
        ),
    ],
    no_gutters=True,
    className="ml-auto flex-nowrap mt-3 mt-md-0",
    align="center",
)

navbar = dbc.Navbar(
    [
        html.A(
            # Use row and col to control vertical alignment of logo / brand
            dbc.Row(
                [
                    #dbc.Col(html.Img(src=PLOTLY_LOGO, height="10px")),
                    dbc.Col(dbc.NavbarBrand("", className="ml-2")),
                ],
                #align="center",
                no_gutters=True,
                justify="center"
            ),
            href="https://plot.ly",
        ),
        dbc.NavbarToggler(id="navbar-toggler"),
        dbc.Collapse(search_bar, id="navbar-collapse", navbar=True),
    ],
    color="#cdd0d4",
    dark=True,
    style={'top':0,
           'left':0,
           'width':'100%',
           'position':'absolute',
          }
)



#app = dash.Dash(
#    external_stylesheets=[dbc.themes.BOOTSTRAP,"https://use.fontawesome.com/releases/v5.7.2/css/all.css"],
#    # these meta_tags ensure content is scaled correctly on different devices
#    meta_tags=[
#        {"name": "viewport", "content": "width=device-width, initial-scale=1"}
#    ],
#)
#app.config.suppress_callback_exceptions = True

#app.scripts.config.serve_locally = True
#app.css.config.serve_locally = True

node_modal = dbc.Modal(
            [
                dbc.ModalHeader("Node"),
                dbc.ModalBody(html.Div(
                                        dcc.Graph(
                                        id='example-graph1',
                                        figure=fig)
                                    )
                ),
                dbc.ModalFooter(
                    dbc.Button(
                        "Close", id="close-centered", className="ml-auto"
                    )
                ),
            ],
            id="modal-centered",
            centered=True,
            backdrop= 'static',
        )

test_modal = dbc.Modal(
            [
                dbc.ModalHeader("Test"),
                dbc.ModalBody(html.Div(
                                        id = 'test-text'
                                    )
                ),
                dbc.ModalFooter(
                    dbc.Button(
                        "Close", id="test_close-centered", className="ml-auto"
                    )
                ),
            ],
            id="test_modal-centered",
            centered=True,
            backdrop= 'static',
        )

search_toast = dbc.Toast(
            "This toast is placed in the top right",
            id="positioned-toast",
            header="Positioned toast",
            is_open=False,
            dismissable=True,
            icon="danger",
            duration = 100,
            # top: 66 positions the toast below the navbar
            style={"position": "fixed", "top": 66, "right": 10, "width": 350},
        )

layout = html.Div([
    navbar, 
    cyto_journey,
    node_modal,
    #test_modal,
    #search_toast,
    #html.Button('Reset', id='bt-reset'),
    ])

@app.callback(Output('cytoscape', 'elements'),                                         
              [Input('cyto-search-button', 'n_clicks_timestamp')],
              [State('cyto-search', 'value'),State('cytoscape', 'elements')])
def update_elements_render(clicks,acc_no, elements):
    if (acc_no is not None) and (clicks != 0):
        try:
            return update_elements(acc_no,clicks, elements)
        except :
            return [{'data': {'id': 'gen-node', 'name':'Invalid Search' , 'weight': 65, 'faveColor': '#cc0e21','faveShape': 'ellipse'}}]
    else:
        raise PreventUpdate
    
@app.callback(Output("modal-centered", "is_open"),
              [Input('cytoscape', 'tapNode'), Input("close-centered", "n_clicks")],
              [State("modal-centered", "is_open")])
def toggle_modal(n1, n2, is_open):
    if (str(n1['data']['id']) != 'gen-node') or n2:
        return not is_open
    return is_open

@app.callback(Output("test_modal-centered", "is_open"),
              [Input('cytoscape', 'tapNode'), Input("test_close-centered", "n_clicks")],
              [State("test_modal-centered", "is_open")])
def toggle_modal(n1, n2, is_open):
    if n1 or n2:
        return not is_open
    return is_open



@app.callback(
    Output("positioned-toast", "is_open"),
    [Input('cyto-search-button', 'n_clicks_timestamp')],
              [State('cyto-search', 'value'),State('cytoscape', 'elements')],
)
def open_toast(n_clicks_timestamp,value,elements):
    if len(elements) < 2:
        return True
    return False

#if __name__ == '__main__':
#    app.run_server(debug=False)
