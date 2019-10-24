import dash
import dash_html_components as html
import dash_core_components as dcc
from dash.dependencies import Input, Output
import plotly.graph_objs as go
import networkx as nx
import pandas as pd
import numpy as np


app = dash.Dash()

query = """
SELECT DISTINCT
ORDER_ID_ANON,
SUBSTR(CAST(CAST(ORDER_ID_ANON AS INT64) AS STRING), 1, 9) AS SERIAL,
MSISDN_ANON,
ORDER_STATUS_DESC,
ACTION_STATUS_DESC,
ACTION_TYPE_DESC 
FROM `bcx-insights.telkom_customerexperience.orders_20190903_00_anon`
GROUP BY ORDER_ID_ANON,SERIAL, MSISDN_ANON,ORDER_STATUS_DESC,ACTION_STATUS_DESC,ACTION_TYPE_DESC
ORDER BY ORDER_ID_ANON,SERIAL, MSISDN_ANON
LIMIT 1000
"""

df = pd.read_gbq(query,
                 project_id='bcx-insights',
                 dialect='standard')

A = list(df["ACTION_TYPE_DESC"].unique())
B = list(df["SERIAL"].unique())
node_list = list(set(A+B))
G = nx.Graph()
for i in node_list:
    G.add_node(i)

for i,j in df.iterrows():
    G.add_edges_from([(j["ACTION_TYPE_DESC"],j["SERIAL"])])

pos = nx.spring_layout(G, k=0.5, iterations=50)
for n, p in pos.items():
    G.node[n]['pos'] = p

##num_nodes = 2000
##edge = df['itemset'][:num_nodes]
#create graph G
##G = nx.Graph()
#G.add_nodes_from(node)
##G.add_edges_from(edge)
#get a x,y position for each node
##pos = nx.layout.spring_layout(G)

#Create Edges
edge_trace = go.Scatter(
    x=[],
    y=[],
    line=dict(width=0.5,color='#888'),
    hoverinfo='none',
    mode='lines')
for edge in G.edges():
    x0, y0 = G.node[edge[0]]['pos']
    x1, y1 = G.node[edge[1]]['pos']
    edge_trace['x'] += tuple([x0, x1, None])
    edge_trace['y'] += tuple([y0, y1, None])

node_trace = go.Scatter(
    x=[],
    y=[],
    text=[],
    mode='markers',
    hoverinfo='text',
    marker=dict(
        showscale=True,
        colorscale='YlGnBu',
        reversescale=True,
        color=[],
        size=10,
        colorbar=dict(
            thickness=15,
            title='Node Connections',
            xanchor='left',
            titleside='right'
        ),  
        line=dict(width=2)))
for node in G.nodes():
    x, y = G.node[node]['pos']
    node_trace['x'] += tuple([x])
    node_trace['y'] += tuple([y])

#add color to node points
for node, adjacencies in enumerate(G.adjacency()):
    node_trace['marker']['color']+=tuple([len(adjacencies[1])])
    node_info = 'Name: ' + str(adjacencies[0]) + '<br># of connections: '+str(len(adjacencies[1]))
    node_trace['text']+=tuple([node_info])

fig = go.Figure(data=[edge_trace, node_trace],
             layout=go.Layout(
                showlegend=False,
                hovermode='closest',
                margin=dict(b=20,l=5,r=5,t=40),
                annotations=[ dict(
                    text="No. of connections",
                    showarrow=False,
                    xref="paper", yref="paper") ],
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))

#iplot(fig)
#plotly.plot(fig)

app.layout = html.Div([
    html.Div([dcc.Graph(id="my-graph", figure = fig)]),
], className="container")

if __name__ == '__main__':
    app.run_server(debug=True)