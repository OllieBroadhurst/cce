import dash
from dash.dependencies import Input, Output
import dash_core_components as dcc
import dash_html_components as html
#from Sunburst_Queries import Overall_Query
from datetime import datetime as dt, timedelta
import pandas as pd
import numpy as np

from app import app

Overall_Query = """with cte as (
SELECT DISTINCT
orders.source,
orders.ORIGINAL_SALES_CHANNEL_DESC,
orders.ACCOUNT_NO_ANON,  
if(disputes.ACCOUNT_NO_ANON is null, 0, 1) has_dispute
FROM `bcx-insights.telkom_customerexperience.orders_20190926_00_anon` orders
left join (SELECT DISTINCT ACCOUNT_NO_ANON FROM `bcx-insights.telkom_customerexperience.disputes_20190903_00_anon`) disputes
on orders.ACCOUNT_NO_ANON = disputes.ACCOUNT_NO_ANON)

select * except(CUSTOMER_NO_ANON) from cte

join  (select 
CUSTOMER_BRAND,
SERVICE_TYPE,
CUSTOMER_NO_ANON from `bcx-insights.telkom_customerexperience.customerdata_20190902_00_anon` 

group by 
CUSTOMER_NO_ANON,
CUSTOMER_BRAND,
SERVICE_TYPE,
CREDIT_CLASS_DESC,
SOURCE) customers

on customers.CUSTOMER_NO_ANON = cte.ACCOUNT_NO_ANON
GROUP BY cte.source,cte.ORIGINAL_SALES_CHANNEL_DESC ,cte.ACCOUNT_NO_ANON ,cte.has_dispute,customer_brand,service_type
ORDER BY cte.source,cte.ORIGINAL_SALES_CHANNEL_DESC DESC
limit 500000
"""

Data_df = pd.read_gbq(Overall_Query,
                project_id = 'bcx-insights',
                dialect = 'standard')
                
                
label = ['Orders']
parent = ['']
value = [len(Data_df)]

for s in Data_df['source'].unique():
  label.append(s)
  parent.append('Orders')
  value.append(len(Data_df[Data_df['source']==s]))

  temp_df = Data_df[Data_df['source']==s]
  for osc in temp_df['ORIGINAL_SALES_CHANNEL_DESC'].unique():
    label.append(osc)
    parent.append(s)
    value.append(len(temp_df[temp_df['ORIGINAL_SALES_CHANNEL_DESC']==osc]))
    
    
import plotly.graph_objects as go

fig =go.Figure(go.Sunburst(
    labels=label,
    parents=parent,
    values=value,
    branchvalues="total",
    maxdepth=2
))

fig.update_layout(margin = dict(t=0, l=0, r=0, b=0))

layout = html.Div([
    html.H1('Telkom Customer Sunburst',style={'text-align':'center'}),
    html.Div([dcc.DatePickerRange(
    display_format='YYYY-MM-DD',
    start_date_placeholder_text='YYYY-MMM-DD',
    end_date_placeholder_text='YYYY-MMM-DD',
    max_date_allowed=dt.today().date(),
    end_date=dt.today().date(),
    start_date=(dt.today() - timedelta(days=30)).date()
    )], style = {'width': '100%', 'display': 'flex', 'align-items': 'center', 'justify-content': 'center'}), 
    html.Div(
        [dcc.Graph(figure=fig)],
        style={'width': '100%', 'display': 'flex', 'align-items': 'center', 'justify-content': 'center', 'padding-top': '10px'}),
    html.Div(id='output', style={'clear': 'both','display': 'flex', 'align-items': 'center', 'justify-content': 'center', 'padding-top': '10px'})
])

@app.callback(Output('output', 'children'), [Input('sun', 'selectedPath')])
def display_selected(selected_path):
    return 'You have selected path: {}'.format('->'.join(selected_path or []) or 'root')

@app.callback(Output('graph', 'figure'), [Input('sun', 'data'), Input('sun', 'selectedPath')])
def display_graph(data, selected_path):
    x = []
    y = []
    text = []
    color = []
    joined_selected = '->'.join(selected_path or [])

    SELECTED_COLOR = '#03c'
    SELECTED_CHILDREN_COLOR = '#8cf'
    SELECTED_PARENTS_COLOR = '#f80'
    DESELECTED_COLOR = '#ccc'

    def node_color(node_path):
        joined_node = '->'.join(node_path)
        if joined_node == joined_selected:
            return SELECTED_COLOR
        if joined_node.startswith(joined_selected):
            return SELECTED_CHILDREN_COLOR
        if joined_selected.startswith(joined_node):
            return SELECTED_PARENTS_COLOR
        return DESELECTED_COLOR

    def append_point(child_count, size, node, node_path):
        x.append(child_count)
        y.append(size)
        text.append(node['name'])
        color.append(node_color(node_path))

    def crawl(node, node_path):
        if 'size' in node:
            append_point(1, node['size'], node, node_path)
            return (1, node['size'])
        else:
            node_count, node_size = 1, 0
            for child in node['children']:
                this_count, this_size = crawl(child, node_path + [child['name']])
                node_count += this_count
                node_size += this_size
            append_point(node_count, node_size, node, node_path)
            return (node_count, node_size)

    crawl(data, [])

    layout = {
        'width': 1000,
        'height': 1000,
        'xaxis': {'title': 'Total Nodes', 'type': 'log'},
        'yaxis': {'title': 'Total Size', 'type': 'log'},
        'hovermode': 'closest'
    }

    return {
        'data': [{
            'x': x,
            'y': y,
            'text': text,
            'textposition': 'middle right',
            'marker': {
                'color': color,
                'size': [(v*v + 100)**0.5 for v in y],
                'opacity': 0.5
            },
            'mode': 'markers+text',
            'cliponaxis': False
        }],
        'layout': layout
    }

