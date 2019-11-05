import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import time

import dash_cytoscape as cyto
import pandas as pd

from apps.tree_chart import get_figure, find_journey, default_chart

from apps.filters import service_options, customer_type, has_dispute, has_fault
from apps.filters import deal_desc, action_status, action_type

from apps.queries import criteria_tree_sql

from app import app
from apps import single_journey,scatter_agg,stages_network


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
                    dbc.NavLink("Overview", href="/page-1", id="page-1-link"),
                    dbc.NavLink("Order Aggregate", href="/page-4", id="page-4-link"),
                    dbc.NavLink("Customer Aggregate", href="/page-2", id="page-2-link"),
                    dbc.NavLink("Single Journey", href="/page-3", id="page-3-link"),
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
        return scatter_agg.layout
    elif pathname == "/page-3":
        #time.sleep(1)
        return single_journey.layout
    elif pathname == "/page-4":
        #time.sleep(1)
        return stages_network.layout
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


if __name__ == '__main__':
    app.run_server(debug=False)
