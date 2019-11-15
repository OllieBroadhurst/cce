import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import time

import dash_cytoscape as cyto
import pandas as pd

#from app import app

elements = [{'data': {'id': 'gen-node', 'name': "Search for an Account Number...", 'weight': 65, 'faveColor': '#095575',
              'faveShape': 'ellipse','text-size':8}}]

cyto_journey = html.Div([
    cyto.Cytoscape(
        id='cytoscape',
        elements=elements,
        zoomingEnabled = True, 
        #zoom = 5,
        layout={
            'name': 'breadthfirst',
            #'roots' : '[id = elements[0]["data"]["id"]]'
            'spacingFactor' : 9,
            'grid' : True,
            #'fit': False,
            'directed' : True,
            
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
    num_devices = -6#len(udevices)
    for device in udevices[num_devices:]:
        id = 'device_' + str(device)
        node = {'data': {'id': id, 'name': id, 'weight': 65, 'faveColor': '#095575',
                'faveShape': 'ellipse'}}
        nodes.append(node)

    # Create Device Edges

    for device in udevices[num_devices:]:
        id = 'device_' + str(device)
        edge = {'data': {'source': 'acc_no_anon_{}'.format(acc_no), 'target': id, 'faveColor': '#60a1bf',
                'strength': 60}}
        edges.append(edge)


    for device in udevices[num_devices:]:
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









PLOTLY_LOGO = "https://images.plot.ly/logo/new-branding/plotly-logomark.png"

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




app = dash.Dash(
    external_stylesheets=[dbc.themes.BOOTSTRAP,"https://use.fontawesome.com/releases/v5.7.2/css/all.css"],
    # these meta_tags ensure content is scaled correctly on different devices
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1"}
    ],
)
app.config.suppress_callback_exceptions = True
app.title = 'Project CCE'


app.layout = html.Div([
    navbar, cyto_journey
    
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


if __name__ == '__main__':
    app.run_server(debug=False) 
