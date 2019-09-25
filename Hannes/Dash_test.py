import dash
import dash_core_components as dcc
import dash_html_components as html
import pandas as pd

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

sql_statement = r"""WITH CTE as (
      SELECT DISTINCT DATE(ORDER_CREATION_DATE) ORDER_CREATION_DATE,
      ORDER_ID_ANON,
      ACTION_TYPE_DESC
       FROM `bcx-insights.telkom_customerexperience.orders_20190903_00_anon`
       WHERE ACCOUNT_NO_ANON = -4302568719576920909
      )
      SELECT *, ROW_NUMBER() OVER (PARTITION BY ORDER_ID_ANON ORDER BY ORDER_CREATION_DATE) Stage
      FROM CTE
      order by ORDER_ID_ANON, ORDER_CREATION_DATE DESC"""


def read_df(sql):
    return pd.io.gbq.read_gbq(sql, project_id='bcx-insights', dialect='standard')
    

df = read_df(sql_statement)
  
def generate_table(dataframe, max_rows=10):
    return html.Table(
        # Header
        [html.Tr([html.Th(col) for col in dataframe.columns])] +

        # Body
        [html.Tr([
            html.Td(dataframe.iloc[i][col]) for col in dataframe.columns
        ]) for i in range(min(len(dataframe), max_rows))]
    )

app.layout = html.Div(children=[
    html.H4(children='Orders Table'),
    dcc.Dropdown(id='dropdown', options=[
        {'label': i, 'value': i} for i in df.ACTION_TYPE_DESC.unique()
    ], multi=True, placeholder='Filter by Action_Type'),
    html.Div(id='table-container')
])


@app.callback(
    dash.dependencies.Output('table-container', 'children'),
    [dash.dependencies.Input('dropdown', 'value')])
def display_table(dropdown_value):
    if dropdown_value is None:
        return generate_table(df)

    dff = df[df.ACTION_TYPE_DESC.str.contains('|'.join(dropdown_value))]
    return generate_table(dff)



if __name__ == '__main__':
    app.run_server(debug=True)