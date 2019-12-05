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

# Load extra layouts
cyto.load_extra_layouts()

tables = {
'customers':'bcx-insights.telkom_customerexperience.customerdata_20191113_anon',
'devices':'bcx-insights.telkom_customerexperience.devices_20191113_anon',
'disputes':'bcx-insights.telkom_customerexperience.disputes_20191113_anon',
'faults':'bcx-insights.telkom_customerexperience.faults_20191113_anon',
'interactions':'bcx-insights.telkom_customerexperience.interactions_20191113_anon',
'orders':'bcx-insights.telkom_customerexperience.orders_20191113_anon',
}


# Default element list for network graph on load.
elements = [{'data': {'id': 'gen-node', 'name': "Search for an Account Number...", 
                      'weight': 65, 
                      'faveColor': '#095575',
                      'faveShape': 'ellipse',
                      'text-size':3,
                      #'position': {'x': 800, 'y': 367}, 
                      #'selectable': False,
                      }}]


# Network graph object visualising journey
cyto_journey = html.Div([
    cyto.Cytoscape(
        id='cytoscape',
        elements=elements,
        zoomingEnabled = True, 
        zoom = 0.5,
        layout={
            'name': 'klay',  # dagre (Vertical) , klay (Horizontal)
            'animate': True, 
            'fit': True,
            'padding': 300,
            'nodeDimensionsIncludeLabels': True,
            'maxSimulationTime': 120*1000,
            'nodeSpacing' : 30000,
            'avoidOverlap': True,
            'refresh':3,
        },
        stylesheet=[{
            'selector': 'node',
            'style': {
                'shape': 'data(faveShape)',
                'width': 'mapData(weight, 1, 100, 1, 200)', 
                'height': 'mapData(weight, 1, 100, 1, 100)',
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




# Function to update network graph elements once CN has been searched for.

def update_elements(acc_no,clicks, elements):
    
    ceases = [
            'Cease due to Collection',
            'Cease Part Of Move',
            'Cease Part of Move',
            'Cease Part of Replace Offer',
            'Cease',
            'Collection Cease',
            'Cease Part Of Migrate',
         ]

    provides = [
            'Provide Same As',
            'Provide Part Of Move And Migrate',
            'Provide Part Of Move',
            'Provide Part of Move',
            'Provide',
            'Provide Part Of Migrate',
          ]
    
    suspends = [
                    'Hard Suspend',
                    'Suspend',
                    'Collection Suspend',
                    'Soft Suspend',
                ]
    orders = pd.io.gbq.read_gbq('''

    SELECT *
    FROM `bcx-insights.telkom_customerexperience.orders_20191113_anon`
    WHERE ACCOUNT_NO_ANON = {}
    

    '''.format(acc_no), project_id='bcx-insights', dialect='standard')


    orders = orders.sort_values(['ACCOUNT_NO_ANON', 'MSISDN_ANON', 'ORDER_ID_ANON', 'ORDER_CREATION_DATE'],ascending=True)

    udevices = orders['MSISDN_ANON'].unique()
    uactions = orders['ACTION_TYPE_DESC'].unique()

  
    nodes = [
                {'data': {'id': 'acc_no_anon_{}'.format(acc_no), 'name':'CN_{}'.format(acc_no),
                'weight': 65, 
                'faveColor': '#550b78',
                'faveShape': 'ellipse'}},
                
                {'data': {'id': 'Orders', 
                'name':'Orders',
                'weight': 65, 
                'faveColor': '#550b78',
                'faveShape': 'ellipse'}},
                
                {'data': {'id': 'Disputes', 
                'name':'Disputes',
                'weight': 65, 
                'faveColor': '#550b78',
                'faveShape': 'ellipse'}},
        ]
    edges = [{'data': {'source': 'acc_no_anon_{}'.format(acc_no), 
                       'target': 'Orders',
                       'faveColor': '#60a1bf',
                       'strength': 60}},
    
             {'data': {'source': 'acc_no_anon_{}'.format(acc_no), 
                       'target': 'Disputes',       
                       'faveColor': '#60a1bf',
                       'strength': 60}}]
    
    n_devices = len(udevices)

    # Create Device Nodes
    
    
    for device in udevices[:n_devices]:
        id = 'device_'  + str(device)
        node = {'data': {'id': id, 'name': id , 'weight': 65, 'faveColor': '#095575',
                'faveShape': 'rectangle'}}
        nodes.append(node)
        
    # Create Device Edges

    for device in udevices[:n_devices]:
        id = 'device_'  + str(device)
        edge = {'data': {'source': 'Orders', 'target': id, 'faveColor': '#60a1bf','strength': 60}}
        edges.append(edge)


    for device in udevices[:n_devices]:
        df = orders[orders['MSISDN_ANON'] == device].sort_values('ORDER_CREATION_DATE')
        # Deals
        for deal in df['DEAL_DESC'].unique():
            id = str(device) +'_'+ str(deal)
            node = {'data': {'id': id, 'name': deal, 'weight': 65, 'faveColor': '#095575',
              'faveShape': 'ellipse'}}
            nodes.append(node)
            cn = node

            edge = {'data': {'source': 'device_' + str(device), 'target': id, 'faveColor': '#60a1bf',
                    'strength': 60}}
            edges.append(edge)
        
        # Actions
            df2 = df[df['DEAL_DESC'] == deal]

            for action in df2['ACTION_TYPE_DESC'].unique():
                
                if action in ceases:
                    faveColor = '#c72314'
                elif action in provides:
                    faveColor = '#23a12c'
                elif action in suspends:
                    faveColor = '#d1961f'
                else :
                    faveColor = '#88a6cf'
                
                node = {'data': {'id': str(device) +'_'+ str(deal) +'_'+ str(action), 'name': action, 'weight': 65, 'faveColor': faveColor,
                    'faveShape': 'ellipse'}}
                nodes.append(node)
      
                edge = {'data': {'source': cn['data']['id'], 'target': str(device) +'_'+ str(deal) +'_'+ str(action), 'faveColor': '#045c16',
                    'strength': 60}}
                edges.append(edge)

                cn = node
                
    # Create dispute nodes + edges
    
    disputes = pd.io.gbq.read_gbq('''

    SELECT *
    FROM `bcx-insights.telkom_customerexperience.disputes_20191113_anon`
    WHERE ACCOUNT_NO_ANON = {}
    

    '''.format(acc_no), project_id='bcx-insights', dialect='standard')
    
    dispute_ids = disputes['DISPUTE_CASE_ID_ANON'].unique()
    
    for dispute_id in dispute_ids:
        node = {'data': {'id': str(dispute_id), 'name': 'ID_' + str(dispute_id), 'weight': 65, 'faveColor': '#095575', 'faveShape': 'rectangle'}}
        nodes.append(node)
        
        edge = {'data': {'source': 'Disputes', 'target': str(dispute_id), 'faveColor': '#60a1bf','strength': 60}}
        edges.append(edge)
        
        cn = node
        
        temp = disputes[disputes['DISPUTE_CASE_ID_ANON'] == dispute_id]
        
        for index,row in temp.iterrows():            
            
            if row['STATUS_OPEN_CLOSE'] in ['Open','Accept','Justified']:
                faveColor = '#2fa84f'
            else:
                faveColor = '#a62821'
            
            node = {'data': {'id': 'ID_' + str(dispute_id) + str(index), 'name': row['REASON_DESC'], 'weight': 65, 'faveColor': faveColor,'faveShape': 'ellipse'}}
            nodes.append(node)
            
            edge = {'data': {'source': cn['data']['id'], 'target': node['data']['id'] , 'faveColor': '#60a1bf','strength': 60}}
        edges.append(edge)
        cn = node
        
    elements = nodes + edges
    return elements


cyto_layout_DD = dropdown = dbc.DropdownMenu(
                            id = 'cyto_layout_DD',
                            label="Layout",
                            children=[
                                dbc.DropdownMenuItem("Horizontal", id = 'H'),
                                dbc.DropdownMenuItem("Vertical", id = 'V'),
                                    ],
                            style = {'top' : 8,
                                     'right' : 305,
                                     'position' : 'absolute'}
)

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
                    dbc.Col(dbc.NavbarBrand("", className="ml-2")),
                ],
                no_gutters=True,
                justify="center"
            ),
            href="https://plot.ly",
        ),
        dbc.NavbarToggler(id="navbar-toggler"),
        dbc.Collapse([cyto_layout_DD,search_bar], id="navbar-collapse", navbar=True),
    ],
    color="#cdd0d4",
    dark=True,
    style={'top':0,
           'left':0,
           'width':'100%',
           'position':'absolute',
          }
)







    

layout = html.Div([
    navbar, 
    cyto_journey,
    
    ])

@app.callback(Output('cytoscape', 'elements'),                                         
              [Input('cyto-search-button', 'n_clicks_timestamp')],
              [State('cyto-search', 'value'),State('cytoscape', 'elements')])
def update_elements_render(clicks,acc_no, elements):
    if (acc_no is not None) and (clicks != 0):
        #elements = []
        return update_elements(acc_no,clicks, elements)
    else:
        raise PreventUpdate
    
@app.callback(Output('cytoscape', 'layout'),
             [Input('H', 'n_clicks'),Input('V', 'n_clicks')],
             [],)
def change_layout(n1,n2):
    # use a dictionary to map ids back to the desired label
    # makes more sense when there are lots of possible labels
    id_lookup = {"H":{
            'name': 'dagre',  # dagre (Vertical) , klay (Horizontal)
            'animate': True, 
            'fit': True,
            'padding': 300,
            'nodeDimensionsIncludeLabels': True,
            'maxSimulationTime': 120*1000,
            'nodeSpacing' : 30000,
            'avoidOverlap': True,
            'refresh':3,
        }, "V": {
            'name': 'klay',  # dagre (Vertical) , klay (Horizontal)
            'animate': True, 
            'fit': True,
            'padding': 300,
            'nodeDimensionsIncludeLabels': True,
            'maxSimulationTime': 120*1000,
            'nodeSpacing' : 30000,
            'avoidOverlap': True,
            'refresh':3,
        }}

    ctx = dash.callback_context

    if (n1 is None and n2 is None) or not ctx.triggered:
        # if neither button has been clicked, return "Not selected"
        return {
            'name': 'klay',  # dagre (Vertical) , klay (Horizontal)
            'animate': True, 
            'fit': True,
            'padding': 300,
            'nodeDimensionsIncludeLabels': True,
            'maxSimulationTime': 120*1000,
            'nodeSpacing' : 30000,
            'avoidOverlap': True,
            'refresh':3,
        }

    # this gets the id of the button that triggered the callback
    button_id = ctx.triggered[0]["prop_id"].split(".")[0]
    return id_lookup[button_id]

if __name__ == '__main__':
    app.run_server(debug=False)
