# -*- coding: utf-8 -*-
#############################################
################# IMPORTS ###################
#############################################
import json

import dash
from dash.dependencies import Input, Output, State
import dash_core_components as dcc
import dash_html_components as html

import dash_cytoscape as cyto
import dash_reusable_components as drc

app = dash.Dash(__name__)
server = app.server

#############################################
############# DATA PROCESSING ###############
#############################################

# Load data
with open('data/sample_network.txt', 'r') as f:
    network_data = f.read().split('\n')

# We select the first 750 edges and associated nodes for an easier visualization
edges = network_data[:750]
nodes = set()

following_node_di = {}  # user id -> list of users they are following
following_edges_di = {}  # user id -> list of cy edges starting from user id

followers_node_di = {}  # user id -> list of followers (cy_node format)
followers_edges_di = {}  # user id -> list of cy edges ending at user id

cy_edges = []
cy_nodes = []

for edge in edges:
    if " " not in edge:
        continue

    source, target = edge.split(" ")

    cy_edge = {'data': {'id': source+target, 'source': source, 'target': target}}
    cy_target = {"data": {"id": target, "label": "User #" + str(target[-5:])}}
    cy_source = {"data": {"id": source, "label": "User #" + str(source[-5:])}}

    if source not in nodes:
        nodes.add(source)
        cy_nodes.append(cy_source)
    if target not in nodes:
        nodes.add(target)
        cy_nodes.append(cy_target)

    # Process dictionary of following
    if not following_node_di.get(source):
        following_node_di[source] = []
    if not following_edges_di.get(source):
        following_edges_di[source] = []

    following_node_di[source].append(cy_target)
    following_edges_di[source].append(cy_edge)

    # Process dictionary of followers
    if not followers_node_di.get(target):
        followers_node_di[target] = []
    if not followers_edges_di.get(target):
        followers_edges_di[target] = []

    followers_node_di[target].append(cy_source)
    followers_edges_di[target].append(cy_edge)

genesis_node = cy_nodes[0]
genesis_node['classes'] = "genesis"
default_elements = [genesis_node]

default_stylesheet = [
    {
        "selector": 'node',
        'style': {
            "opacity": 0.65,
            'z-index': 9999
        }
    },
    {
        "selector": 'edge',
        'style': {
            "curve-style": "bezier",
            "opacity": 0.45,
            'z-index': 5000
        }
    },
    {
        'selector': '.followerNode',
        'style': {
            'background-color': '#0074D9'
        }
    },
    {
        'selector': '.followerEdge',
        "style": {
            "mid-target-arrow-color": "blue",
            "mid-target-arrow-shape": "vee",
            "line-color": "#0074D9"
        }
    },
    {
        'selector': '.followingNode',
        'style': {
            'background-color': '#FF4136'
        }
    },
    {
        'selector': '.followingEdge',
        "style": {
            "mid-target-arrow-color": "red",
            "mid-target-arrow-shape": "vee",
            "line-color": "#FF4136",
        }
    },
    {
        "selector": '.genesis',
        "style": {
            'background-color': '#B10DC9',
            "border-width": 2,
            "border-color": "purple",
            "border-opacity": 1,
            "opacity": 1,

            "label": "data(label)",
            "color": "#B10DC9",
            "text-opacity": 1,
            "font-size": 12,
            'z-index': 9999
        }
    },
    {
        'selector': ':selected',
        "style": {
            "border-width": 2,
            "border-color": "black",
            "border-opacity": 1,
            "opacity": 1,
            "label": "data(label)",
            "color": "black",
            "font-size": 12,
            'z-index': 9999
        }
    }
]





for network_edge in edges:
    source, target = network_edge.split(" ")

    if source not in nodes:
        nodes.add(source)
        cy_nodes.append({"data": {"id": source, "label": "User #" + source[-5:]}})
    if target not in nodes:
        nodes.add(target)
        cy_nodes.append({"data": {"id": target, "label": "User #" + target[-5:]}})

    cy_edges.append({
        'data': {
            'source': source,
            'target': target
        }
    })


basic_elements = [
    {
        'data': {'id': 'one', 'label': 'Node 1'},
        'position': {'x': 50, 'y': 50}
    },
    {
        'data': {'id': 'two', 'label': 'Node 2'},
        'position': {'x': 200, 'y': 200}
    },
    {
        'data': {'id': 'three', 'label': 'Node 3'},
        'position': {'x': 100, 'y': 150}
    },
    {
        'data': {'id': 'four', 'label': 'Node 4'},
        'position': {'x': 400, 'y': 50}
    },
    {
        'data': {'id': 'five', 'label': 'Node 5'},
        'position': {'x': 250, 'y': 100}
    },
    {
        'data': {'id': 'six', 'label': 'Node 6', 'parent': 'three'},
        'position': {'x': 150, 'y': 150}
    },
    {
        'data': {
            'id': 'one-two',
            'source': 'one',
            'target': 'two',
            'label': 'Edge from Node1 to Node2'
        }
    },
    {
        'data': {
            'id': 'one-five',
            'source': 'one',
            'target': 'five',
            'label': 'Edge from Node 1 to Node 5'
        }
    },
    {
        'data': {
            'id': 'two-four',
            'source': 'two',
            'target': 'four',
            'label': 'Edge from Node 2 to Node 4'
        }
    },
    {
        'data': {
            'id': 'three-five',
            'source': 'three',
            'target': 'five',
            'label': 'Edge from Node 3 to Node 5'
        }
    },
    {
        'data': {
            'id': 'three-two',
            'source': 'three',
            'target': 'two',
            'label': 'Edge from Node 3 to Node 2'
        }
    },
    {
        'data': {
            'id': 'four-four',
            'source': 'four',
            'target': 'four',
            'label': 'Edge from Node 4 to Node 4'
        }
    },
    {
        'data': {
            'id': 'four-six',
            'source': 'four',
            'target': 'six',
            'label': 'Edge from Node 4 to Node 6'
        }
    },
    {
        'data': {
            'id': 'five-one',
            'source': 'five',
            'target': 'one',
            'label': 'Edge from Node 5 to Node 1'
        }
    },
]

styles3 = {
    'json-output': {
        'overflow-y': 'scroll',
        'height': 'calc(50% - 25px)',
        'border': 'thin lightgrey solid'
    },
    'tab': {'height': 'calc(98vh - 115px)'}
}




styles = {
    'json-output': {
        'overflow-y': 'scroll',
        'height': 'calc(50% - 25px)',
        'border': 'thin lightgrey solid'
    },
    'tab': {'height': 'calc(98vh - 80px)'}
}

#############################################
################# TESTING ###################
#############################################

cce_elements = []

#############################################
################# LAYOUT ####################
#############################################

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)
app.title = 'Project CCE'
app.layout = html.Div([
    html.Div([
        html.H3('Telkom Insights - Customer Journey')
    ]),
    dcc.Tabs(id="tabsOuter", children=[
        dcc.Tab(label='Expand', children=[
            
            
            html.Div([
    html.Div(className='eight columns', children=[
        cyto.Cytoscape(
            id='cytoscape',
            elements=default_elements,
            stylesheet=default_stylesheet,
            zoom = 100,
            style={
                'height': '95vh',
                'width': '100%',
                'float': 'left'
            }
        )
    ]),

    html.Div(className='four columns', children=[
        dcc.Tabs(id='tabs1', children=[
            dcc.Tab(label='Control Panel', children=[
                drc.NamedDropdown(
                    name='Layout',
                    id='dropdown-layout',
                    options=drc.DropdownOptionsList(
                        'random',
                        'grid',
                        'circle',
                        'concentric',
                        'breadthfirst',
                        'cose'
                    ),
                    value='grid',
                    clearable=False
                ),
                drc.NamedRadioItems(
                    name='Expand',
                    id='radio-expand',
                    options=drc.DropdownOptionsList(
                        'followers',
                        'following'
                    ),
                    value='followers'
                )
            ]),

            dcc.Tab(label='Data', children=[
                html.Div(style=styles['tab'], children=[
                    html.P('Node Object JSON:'),
                    html.Pre(
                        id='tap-node-json-output',
                        style=styles['json-output']
                    ),
                    html.P('Edge Object JSON:'),
                    html.Pre(
                        id='tap-edge-json-output',
                        style=styles['json-output']
                    )
                ])
            ])
        ]),

    ])
])
            
            
            
        ]),
        
        
        dcc.Tab(label='Highlight', children=[
            
            
            
            
             html.Div([
    html.Div(className='eight columns', children=[
        cyto.Cytoscape(
            id='cytoscape2',
            elements=cy_edges + cy_nodes,
            style={
                'height': '95vh',
                'width': '100%',
                'float' : 'left'
            }
        )
    ]),

    html.Div(className='four columns', children=[
        dcc.Tabs(id='tabs2', children=[
            dcc.Tab(label='Control Panel', children=[
                drc.NamedDropdown(
                    name='Layout',
                    id='dropdown-layout2',
                    options=drc.DropdownOptionsList(
                        'random',
                        'grid',
                        'circle',
                        'concentric',
                        'breadthfirst',
                        'cose'
                    ),
                    value='random',
                    clearable=False
                ),

                drc.NamedDropdown(
                    name='Node Shape',
                    id='dropdown-node-shape2',
                    value='ellipse',
                    clearable=False,
                    options=drc.DropdownOptionsList(
                        'ellipse',
                        'triangle',
                        'rectangle',
                        'diamond',
                        'pentagon',
                        'hexagon',
                        'heptagon',
                        'octagon',
                        'star',
                        'polygon',
                    )
                ),

                drc.NamedInput(
                    name='Incoming Color',
                    id='input-follower-color',
                    type='text',
                    value='blue',
                ),

                drc.NamedInput(
                    name='Outgoing Color',
                    id='input-following-color',
                    type='text',
                    value='red',
                ),
            ]),

            dcc.Tab(label='Data', children=[
                html.Div(style=styles['tab'], children=[
                    html.P('Node Object JSON:'),
                    html.Pre(
                        id='tap-node-json-output2',
                        style=styles['json-output']
                    ),
                    html.P('Edge Object JSON:'),
                    html.Pre(
                        id='tap-edge-json-output2',
                        style=styles['json-output']
                    )
                ])
            ])
        ]),
    ])
])
            
            
            
            
            
        ]),
        
        
        
        
        dcc.Tab(label='Empty', children=[
            html.Div([
    html.Div(className='eight columns', children=[
        cyto.Cytoscape(
            id='cytoscape3',
            elements=basic_elements,
            layout={
                'name': 'preset'
            },
            style={
                'height': '95vh',
                'width': '100%'
            }
        )
    ]),

    html.Div(className='four columns', children=[
        dcc.Tabs(id='tabs3', children=[
            dcc.Tab(label='Tap Objects', children=[
                html.Div(style=styles['tab'], children=[
                    html.P('Node Object JSON:'),
                    html.Pre(
                        id='tap-node-json-output3',
                        style=styles3['json-output']
                    ),
                    html.P('Edge Object JSON:'),
                    html.Pre(
                        id='tap-edge-json-output3',
                        style=styles3['json-output']
                    )
                ])
            ]),

            dcc.Tab(label='Tap Data', children=[
                html.Div(style=styles3['tab'], children=[
                    html.P('Node Data JSON:'),
                    html.Pre(
                        id='tap-node-data-json-output3',
                        style=styles3['json-output']
                    ),
                    html.P('Edge Data JSON:'),
                    html.Pre(
                        id='tap-edge-data-json-output3',
                        style=styles3['json-output']
                    )
                ])
            ]),

            dcc.Tab(label='Mouseover Data', children=[
                html.Div(style=styles3['tab'], children=[
                    html.P('Node Data JSON:'),
                    html.Pre(
                        id='mouseover-node-data-json-output3',
                        style=styles3['json-output']
                    ),
                    html.P('Edge Data JSON:'),
                    html.Pre(
                        id='mouseover-edge-data-json-output3',
                        style=styles3['json-output']
                    )
                ])
            ]),
            dcc.Tab(label='Selected Data', children=[
                html.Div(style=styles['tab'], children=[
                    html.P('Node Data JSON:'),
                    html.Pre(
                        id='selected-node-data-json-output3',
                        style=styles3['json-output']
                    ),
                    html.P('Edge Data JSON:'),
                    html.Pre(
                        id='selected-edge-data-json-output3',
                        style=styles3['json-output']
                    )
                ])
            ])
        ]),

    ])
])

            
        ]),
    
    
    
    
    ])
])



#############################################
################# CALLBACKS #################
#############################################

@app.callback(Output('tap-node-json-output', 'children'),
              [Input('cytoscape', 'tapNode')])
def display_tap_node(data):
    return json.dumps(data, indent=2)


@app.callback(Output('tap-edge-json-output', 'children'),
              [Input('cytoscape', 'tapEdge')])
def display_tap_edge(data):
    return json.dumps(data, indent=2)


@app.callback(Output('cytoscape', 'layout'),
              [Input('dropdown-layout', 'value')])
def update_cytoscape_layout(layout):
    return {'name': layout}


@app.callback(Output('cytoscape', 'elements'),
              [Input('cytoscape', 'tapNodeData')],
              [State('cytoscape', 'elements'),
               State('radio-expand', 'value')])
def generate_elements(nodeData, elements, expansion_mode):
    if not nodeData:
        return default_elements

    # If the node has already been expanded, we don't expand it again
    if nodeData.get('expanded'):
        return elements

    # This retrieves the currently selected element, and tag it as expanded
    for element in elements:
        if nodeData['id'] == element.get('data').get('id'):
            element['data']['expanded'] = True
            break

    if expansion_mode == 'followers':

        followers_nodes = followers_node_di.get(nodeData['id'])
        followers_edges = followers_edges_di.get(nodeData['id'])

        if followers_nodes:
            for node in followers_nodes:
                node['classes'] = 'followerNode'
            elements.extend(followers_nodes)

        if followers_edges:
            for follower_edge in followers_edges:
                follower_edge['classes'] = 'followerEdge'
            elements.extend(followers_edges)

    elif expansion_mode == 'following':

        following_nodes = following_node_di.get(nodeData['id'])
        following_edges = following_edges_di.get(nodeData['id'])

        if following_nodes:
            for node in following_nodes:
                if node['data']['id'] != genesis_node['data']['id']:
                    node['classes'] = 'followingNode'
                    elements.append(node)

        if following_edges:
            for follower_edge in following_edges:
                follower_edge['classes'] = 'followingEdge'
            elements.extend(following_edges)

    return elements











@app.callback(Output('tap-node-json-output2', 'children'),
              [Input('cytoscape2', 'tapNode')])
def display_tap_node2(data):
    return json.dumps(data, indent=2)


@app.callback(Output('tap-edge-json-output2', 'children'),
              [Input('cytoscape2', 'tapEdge')])
def display_tap_edge2(data):
    return json.dumps(data, indent=2)


@app.callback(Output('cytoscape2', 'layout'),
              [Input('dropdown-layout2', 'value')])
def update_cytoscape_layout2(layout):
    return {'name': layout}


@app.callback(Output('cytoscape2', 'stylesheet'),
              [Input('cytoscape2', 'tapNode'),
               Input('input-follower-color', 'value'),
               Input('input-following-color', 'value'),
               Input('dropdown-node-shape2', 'value')])
def generate_stylesheet2(node, follower_color, following_color, node_shape):
    if not node:
        return default_stylesheet

    stylesheet = [{
        "selector": 'node',
        'style': {
            'opacity': 0.3,
            'shape': node_shape
        }
    }, {
        'selector': 'edge',
        'style': {
            'opacity': 0.2,
            "curve-style": "bezier",
        }
    }, {
        "selector": 'node[id = "{}"]'.format(node['data']['id']),
        "style": {
            'background-color': '#B10DC9',
            "border-color": "purple",
            "border-width": 2,
            "border-opacity": 1,
            "opacity": 1,

            "label": "data(label)",
            "color": "#B10DC9",
            "text-opacity": 1,
            "font-size": 12,
            'z-index': 9999
        }
    }]

    for edge in node['edgesData']:
        if edge['source'] == node['data']['id']:
            stylesheet.append({
                "selector": 'node[id = "{}"]'.format(edge['target']),
                "style": {
                    'background-color': following_color,
                    'opacity': 0.9
                }
            })
            stylesheet.append({
                "selector": 'edge[id= "{}"]'.format(edge['id']),
                "style": {
                    "mid-target-arrow-color": following_color,
                    "mid-target-arrow-shape": "vee",
                    "line-color": following_color,
                    'opacity': 0.9,
                    'z-index': 5000
                }
            })

        if edge['target'] == node['data']['id']:
            stylesheet.append({
                "selector": 'node[id = "{}"]'.format(edge['source']),
                "style": {
                    'background-color': follower_color,
                    'opacity': 0.9,
                    'z-index': 9999
                }
            })
            stylesheet.append({
                "selector": 'edge[id= "{}"]'.format(edge['id']),
                "style": {
                    "mid-target-arrow-color": follower_color,
                    "mid-target-arrow-shape": "vee",
                    "line-color": follower_color,
                    'opacity': 1,
                    'z-index': 5000
                }
            })

    return stylesheet










@app.callback(Output('tap-node-json-output3', 'children'),
              [Input('cytoscape3', 'tapNode')])
def displayTapNode(data):
    return json.dumps(data, indent=2)


@app.callback(Output('tap-edge-json-output3', 'children'),
              [Input('cytoscape3', 'tapEdge')])
def displayTapEdge(data):
    return json.dumps(data, indent=2)


@app.callback(Output('tap-node-data-json-output3', 'children'),
              [Input('cytoscape3', 'tapNodeData')])
def displayTapNodeData(data):
    return json.dumps(data, indent=2)


@app.callback(Output('tap-edge-data-json-output3', 'children'),
              [Input('cytoscape3', 'tapEdgeData')])
def displayTapEdgeData(data):
    return json.dumps(data, indent=2)


@app.callback(Output('mouseover-node-data-json-output3', 'children'),
              [Input('cytoscape3', 'mouseoverNodeData')])
def displayMouseoverNodeData(data):
    return json.dumps(data, indent=2)


@app.callback(Output('mouseover-edge-data-json-output3', 'children'),
              [Input('cytoscape3', 'mouseoverEdgeData')])
def displayMouseoverEdgeData(data):
    return json.dumps(data, indent=2)


@app.callback(Output('selected-node-data-json-output3', 'children'),
              [Input('cytoscape3', 'selectedNodeData')])
def displaySelectedNodeData(data):
    return json.dumps(data, indent=2)


@app.callback(Output('selected-edge-data-json-output3', 'children'),
              [Input('cytoscape3', 'selectedEdgeData')])
def displaySelectedEdgeData(data):
    return json.dumps(data, indent=2)


#############################################
################# DEPLOY ####################
#############################################

if __name__ == "__main__":
    app.run_server(debug=False, port = 8051)
    
    
