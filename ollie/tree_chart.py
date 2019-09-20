#from google.auth.credentials import Signing
#credentials = Signing()#app_engine.Credentials()
#print('Authenticated')

#from google.cloud import bigquery
import networkx as nx
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import numpy as np
import pandas as pd

#client = bigquery.Client('bcx-insights')

tree_sql = r"""WITH CTE as (
      SELECT DISTINCT DATE(ORDER_CREATION_DATE) ORDER_CREATION_DATE,
      ORDER_ID_ANON,
      ACTION_TYPE_DESC
       FROM `bcx-insights.telkom_customerexperience.orders_20190903_00_anon`
       WHERE ACCOUNT_NO_ANON = 5997992223372343676
      )

      SELECT *, ROW_NUMBER() OVER (PARTITION BY ORDER_ID_ANON ORDER BY ORDER_CREATION_DATE) Stage
      FROM CTE
      order by ORDER_ID_ANON, ORDER_CREATION_DATE DESC"""


def tree_chart(sql):
    df = pd.io.gbq.read_gbq(tree_sql, project_id='bcx-insights', dialect='standard')
    df['ACTION_TYPE_DESC'] = df['ACTION_TYPE_DESC'].fillna('Other')

    WIDTH = df['Stage'].value_counts().max()#50
    HEIGHT = 5#10#len(df['Stage'].drop_duplicates()) #* 10

    TOP = HEIGHT * 0.9
    LEVEL_HEIGHT = HEIGHT // len(df)

    w_spacing = (WIDTH/df[['ACTION_TYPE_DESC', 'Stage']].drop_duplicates()['Stage'].value_counts()).round(2).to_dict()
    h_spacing = round(HEIGHT/(df['Stage'].max()), 2) * 0.85

    labels = {}
    coords = {}
    counts = {}
    coords_map = {}

    k = 0
    for i in range(df['Stage'].min(), df['Stage'].max() + 1):
        reasons = df.loc[df['Stage'] == i, 'ACTION_TYPE_DESC'].value_counts().to_dict()

        for j, r in enumerate(reasons.keys()):
            counts[k] = reasons[r]
            labels[k] = r
            coords[k] = (w_spacing[i] * (j + 0.5), TOP - i * h_spacing)
            coords_map[(i, r)] = coords[k]
            k += 1

    all_nodes = list(coords.values())

    coords_map = pd.Series(coords_map, name='Position')

    df = pd.merge(df, pd.Series(coords_map), how='left', left_on=['Stage', 'ACTION_TYPE_DESC'], right_index=True)

    df = df[['ORDER_ID_ANON', 'Stage', 'Position']].groupby(['ORDER_ID_ANON', 'Stage']).first()
    df['Link'] = df['Position'].shift(-1)

    links = df[['Position', 'Link']].dropna().drop_duplicates().values

    colours = ['green'] * len(all_nodes)

    node_x = [x[0] for x in all_nodes]
    node_y = [y[1] for y in all_nodes]

    edge_x = []
    edge_y = []

    if link[0][1] > link[1][1]:
        edge_x.append(link[0][0])
        edge_x.append(link[1][0])
        edge_y.append(link[0][1])
        edge_y.append(link[1][1])

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='#888'),
        hoverinfo='none',
        mode='lines')

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        textposition='top center',
        text = list(labels.values()),
        hoverinfo='text',
        hovertext=[str(t) for t in counts.values()],
        marker=dict(
            color=colours,
            size=10,
            line_width=2))

    fig = go.FigureWidget(data=[edge_trace, node_trace],
                 layout=go.Layout(
                    titlefont_size=16,
                    showlegend=False,
                    #hovermode='closest',
                    margin=dict(b=20,l=5,r=5,t=40),
                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                    )


    return fig
