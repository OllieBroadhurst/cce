#-----------------------------------------------------------------------------------------------#
#----------------------------IMPORTS------------------------------------------------------------#
#-----------------------------------------------------------------------------------------------#
import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import time

import dash_cytoscape as cyto
import pandas as pd

# Ollie -- Start
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
# Ollie -- End

#-----------------------------------------------------------------------------------------------#
#----------------------------COMPONENTS---------------------------------------------------------#
#-----------------------------------------------------------------------------------------------#

# Index Page--Start

app = dash.Dash(
    external_stylesheets=[dbc.themes.BOOTSTRAP,"https://use.fontawesome.com/releases/v5.7.2/css/all.css"],
    # these meta_tags ensure content is scaled correctly on different devices
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1"}
    ],
)
app.title = 'Project CCE'
sidebar_header = dbc.Row(
    [
        dbc.Col(html.Img(src='https://static.telkom.co.za/today/static/web/img/icons/png/logo.png')),
        dbc.Col(
            [
                html.Button(
                    # use the Bootstrap navbar-toggler classes to style
                    html.Span(className="navbar-toggler-icon"),
                    className="navbar-toggler",
                    # the navbar-toggler classes don't set color
                    style={
                        "color": "rgba(0,0,0,.5)",
                        "border-color": "rgba(0,0,0,.1)",
                    },
                    id="navbar-toggle",
                ),
                html.Button(
                    # use the Bootstrap navbar-toggler classes to style
                    html.Span(className="navbar-toggler-icon"),
                    className="navbar-toggler",
                    # the navbar-toggler classes don't set color
                    style={
                        "color": "rgba(0,0,0,.5)",
                        "border-color": "rgba(0,0,0,.1)",
                    },
                    id="sidebar-toggle",
                ),
            ],
            # the column containing the toggle will be only as wide as the
            # toggle, resulting in the toggle being right aligned
            width="auto",
            # vertically align the toggle in the center
            align="center",
        ),
    ]
)

sidebar = html.Div(
    [
        sidebar_header,
        # we wrap the horizontal rule and short blurb in a div that can be
        # hidden on a small screen
        html.Div(
            [
                html.Hr(),
                html.P(
                    "Customer Journey",
                    className="lead",
                    style = {'text-align': 'center'},
                ),
            ],
            id="blurb",
        ),
        # use the Collapse component to animate hiding / revealing links
        dbc.Collapse(
            dbc.Nav(
                [
                    dbc.NavLink("NetworkX", href="/page-1", id="page-1-link"),
                    dbc.NavLink("Plotly", href="/page-2", id="page-2-link"),
                    dbc.NavLink("Cytoscape", href="/page-3", id="page-3-link"),
                ],
                vertical=True,
                pills=True,
            ),
            id="collapse",
        ),
    ],
    id="sidebar",
)

#content = html.Div(id="page-content")

content = dcc.Loading(id="content-loading", 
                      children=[html.Div(id="page-content")],
                      #fullscreen=True,
                      #style={'position': 'absolute'}
                      )

app.layout = html.Div([dcc.Location(id="url"), sidebar, content])
app.title = 'Project CCE'
app.config.suppress_callback_exceptions = True

# Index Page--End


# Cytoscape Page--Start


elements = [{'data': {'id': 'gen-node', 'name': "Search for an Account Number...", 'weight': 65, 'faveColor': '#095575',
              'faveShape': 'ellipse','text-size':8}}]

cyto_journey = html.Div([
    cyto.Cytoscape(
        id='cytoscape',
        elements=elements,
        zoomingEnabled = True, 
        zoom = 5,
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
                    dbc.Col(html.Img(src=PLOTLY_LOGO, height="10px")),
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








single_journey = html.Div([
    navbar, cyto_journey
    
    ])
# Cytoscape Page--End

# Ollie -- Start

header_style = {'font-size': '16px'}
filter_padding_top = '20px'
filter_float = 'right'
filter_height = '48px'
filter_padding_right = '30px'
filter_font_size = '12px'

graph = html.Div(dcc.Graph(id='tree_chart',
                    figure=default_chart(), style={'overflow-y': 'hidden'}),
                    style={'padding-top': '1%', 'padding-right': '2%',
                        'float': 'right', 'width': '100%', 'height': '90%'}
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
                                'padding-right': filter_padding_right})])

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
                                'padding-right': filter_padding_right})], 
                        style={'padding-top': '2px'})

button = html.Button('Run', id='run_button', style={'float': 'right'})

collapse_button = html.Div(html.Button(className='mr-1',
                    children=html.Span(
                                html.I(className='fas fa-angle-up',
                                        id='button-icon',
                                        style={'aria-hidden': 'true'})),
                    id='collapse-button'),
                style={'padding-left': '50px', 'padding-right': '50px', 'float': 'right'})


filter_panel_style = {'float': 'right', 'width': '910px', 'height': '250px'}
filter_panel =  dbc.Collapse(children=dbc.Card([button, top_filters, bottom_filters], style=filter_panel_style),
                                    id='collapse2',
                                    is_open=True)

FONT_AWESOME = "https://use.fontawesome.com/releases/v5.7.2/css/all.css"

outputs = [Output('tree_chart', 'figure'), Output('history', 'children'),
            Output('links', 'children'), Output('routes', 'children')]

inputs = [Input('run_button', 'n_clicks'), Input('tree_chart', 'clickData')]

states = [State('service_filter', 'value'), State('customer_type_filter', 'value'),
        State('history', 'children'), State('tree_chart', 'figure'),
        State('links', 'children'), State('routes', 'children'),
        State('deal_desc_filter', 'value'), State('action_status_filter', 'value'),
        State('date_filter', 'start_date'), State('date_filter', 'end_date'),
        State('dispute_filter', 'value'), State('final_action_filter', 'value'),
        State('fault_filter', 'value')]

ollie_layout = html.Div([
            html.Div([collapse_button, filter_panel], style={'width': '90%', 'padding-top': '5px', 'float': 'right'}),
            graph,
            html.Div(children='{}', id='history', style={'display': 'none'}),
            html.Div(children='{}', id='links', style={'display': 'none'}),
            html.Div(children='{}', id='routes', style={'display': 'none'}),
            html.Div(children='{}', id='desc_map', style={'display': 'none'})
            ],
            style={'overflow-y': 'hidden'})
# Ollie -- End

#-----------------------------------------------------------------------------------------------#
#----------------------------CALLBACKS----------------------------------------------------------#
#-----------------------------------------------------------------------------------------------#

# Index Page--Start

# this callback uses the current pathname to set the active state of the
# corresponding nav link to true, allowing users to tell which page they are on.
@app.callback(
    [Output(f"page-{i}-link", "active") for i in range(1, 4)],
    [Input("url", "pathname")],
)
def toggle_active_links(pathname):
    if pathname == "/":
        # Treat page 1 as the homepage / index
        return True, False, False
    return [pathname == f"/page-{i}" for i in range(1, 4)]


@app.callback(Output("page-content", "children"), [Input("url", "pathname")])
def render_page_content(pathname):
    if pathname in ["/", "/page-1"]:
        #time.sleep(1)
        return html.P("This is the content of page 1!")
    elif pathname == "/page-2":
        #time.sleep(1)
        return ollie_layout #html.P("This is the content of page 2!")
    elif pathname == "/page-3":
        #time.sleep(1)
        return single_journey
    # If the user tries to reach a different page, return a 404 message
    return dbc.Jumbotron(
        [
            html.H1("404: Not found", className="text-danger"),
            html.Hr(),
            html.P(f"The pathname {pathname} was not recognised..."),
        ]
    )


@app.callback(
    Output("sidebar", "className"),
    [Input("sidebar-toggle", "n_clicks")],
    [State("sidebar", "className")],
)
def toggle_classname(n, classname):
    if n and classname == "":
        return "collapsed"
    return ""


@app.callback(
    Output("collapse", "is_open"),
    [Input("navbar-toggle", "n_clicks")],
    [State("collapse", "is_open")],
)
def toggle_collapse(n, is_open):
    if n:
        return not is_open
    return is_open

# Index Page--End

# Cytoscape Page--Start
@app.callback(Output('cytoscape', 'elements'),                                         
              [Input('cyto-search-button', 'n_clicks_timestamp')],
              [State('cyto-search', 'value'),State('cytoscape', 'elements')])
def update_elements_render(clicks,acc_no, elements):
    if (acc_no is not None) and (clicks != 0):
        #elements = []
        return update_elements(acc_no,clicks, elements)
    else:
        raise PreventUpdate
# Cytoscape Page--End

# Ollie -- Start
@app.callback(outputs, inputs, states)
def generate_tree(click_btn, node_click, services, types,
                btn_history, current_fig, links, routes,
                deals, status, start_date_val, end_date_val, dispute_val,
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
                                        start_date_val, end_date_val, dispute_val,
                                        action_filter, fault_filter)

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

# Olle -- End

#-----------------------------------------------------------------------------------------------#
#----------------------------DEPLOY-------------------------------------------------------------#
#-----------------------------------------------------------------------------------------------#
if __name__ == "__main__":
    app.run_server(port=8888, debug=False)
