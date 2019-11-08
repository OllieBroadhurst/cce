

import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
#from dash.dependencies import Input, Output, State
#from dash.exceptions import PreventUpdate


from modeling import get_bar_graph, default_risk_graph


graph = html.Div(dcc.Graph(id='risk_chart',
                           figure=get_bar_graph(), style={'overflow-y': 'hidden'}),
                 style={'padding-top': '5px', 'padding-right': '5px',
                        'float': 'right', 'width': '100%'}
                 )


FONT_AWESOME = "https://use.fontawesome.com/releases/v5.7.2/css/all.css"
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, FONT_AWESOME])

app.layout = html.Div([graph])








if __name__ == '__main__':
    app.run_server(debug=True)
