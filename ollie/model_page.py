
import dash
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import dash_table
import json

from modeling import get_bar_graph, default_risk_graph

button = html.Div(html.Button('Run', id='predict_button', style={'float': 'right'}),
        style={'padding-right': '15px', 'padding-top': '15px'})

graph = html.Div(dcc.Graph(id='risk_chart',
                           figure=default_risk_graph(),
                           style={'overflow-y': 'hidden',
                                    'overflow-x': 'hidden'}),
                 style={'padding-top': '5px', 'padding-right': '5px',
                        'float': 'right', 'width': '80%'}
                 )

table_data = html.Div(children='{}', id='table_data', style={'display': 'none'})

table = html.Div(dash_table.DataTable(
        id='model_table',
        columns=[{"name": '', "id": ''}],
        data=[],
        style_table={'overflowY': 'scroll', 'height': '30vh'},
        style_cell = {
                'font_family': 'tahoma',
                'font_size': '12px',
                'text_align': 'right'
            }),
        style={'padding-top': '5px', 'width': '80%',
            'float': 'right', 'padding-right': '15px'})

FONT_AWESOME = "https://use.fontawesome.com/releases/v5.7.2/css/all.css"
app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP, FONT_AWESOME])

app.layout = html.Div([button, graph, table, table_data])


@app.callback(
    [Output("risk_chart", "figure"),
    Output("model_table", "columns"),
    Output("table_data", "children")],
    [Input("predict_button", "n_clicks")])
def get_chart(n):
    if n is not None:
        fig, table_data = get_bar_graph()
        table_columns = [{"name": i, "id": i} for i in table_data.columns]
        table_records = table_data.to_dict('records')
        table_records = json.dumps(table_records)
        return fig, table_columns, table_records
    else:
        raise PreventUpdate

@app.callback([Output('model_table', 'data')],
            [Input('risk_chart', 'clickData')],
            [State('table_data', 'children')])
def display_data(clicked_data, data):
    if clicked_data is not None:
        data = json.loads(data)


        lower_lim = float(clicked_data['points'][0]['x'].split()[0])
        upper_lim = float(clicked_data['points'][0]['x'].split()[-1])

        table_data = []
        for i in data:
            if i['probability'] > lower_lim and i['probability'] <= upper_lim:
                i['probability'] = round(i['probability'], 5)
                table_data.append(i)

        return [table_data]
    else:
        raise PreventUpdate

if __name__ == '__main__':
    app.run_server(debug=True)
