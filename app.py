import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
import time

import dash_cytoscape as cyto
import pandas as pd

# Initial app renderer.
app = dash.Dash(
    external_stylesheets=[dbc.themes.BOOTSTRAP,"https://use.fontawesome.com/releases/v5.7.2/css/all.css"],
    # these meta_tags ensure content is scaled correctly on different devices
    meta_tags=[
        {"name": "viewport", "content": "width=device-width, initial-scale=1"}
    ],
)
# Ensure standalone apps can execute callbacks found in their scripts via index.py
app.config.suppress_callback_exceptions = True
