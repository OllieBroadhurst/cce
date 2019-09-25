
#import networkx as nx
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import numpy as np
import pandas as pd

from queries import default_tree_sql, criteria_tree_sql


class tree_chart:
    def __init__(self, service_types=None):

        self.service_types = service_types
        self.sql = criteria_tree_sql(self.service_types)
        self.figure = go.FigureWidget({'data':
                    {
                    'x': [],
                    'y': [],
                    'mode': 'markers',
                    'marker': {'size': 1}
                    },
                    'layout':go.Layout(
                    xaxis=dict(showticklabels=False, showline=False),
                    yaxis=dict(showticklabels=False, showline=False)
                    )})

        self.num_nodes = 0
        self.colours = []
        self.links = {}

    def get_figure(self):
        df = pd.io.gbq.read_gbq(self.sql, project_id='bcx-insights', dialect='standard')

        if len(df) == 0:
            return go.FigureWidget({'data':
                        {
                        'x': [],
                        'y': [],
                        'mode': 'markers',
                        'marker': {'size': 1}
                        },
                        'layout':go.Layout(
                        xaxis=dict(showticklabels=False, showline=False),
                        yaxis=dict(showticklabels=False, showline=False)
                        )})

        df['ACTION_TYPE_DESC'] = df['ACTION_TYPE_DESC'].fillna('Other')

        WIDTH = df['Stage'].value_counts().max()
        HEIGHT = 5

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

        for ix, l in df.iterrows():
          if type(l['Link']) is tuple and l['Position'][1] > l['Link'][1]:
            if l['Position'] not in self.links.keys():
              self.links[l['Position']] = []

            if l['Link'] not in self.links[l['Position']]:
              self.links[l['Position']].append(l['Link'])

        colours = ['green'] * len(all_nodes)

        node_x = [x[0] for x in all_nodes]
        node_y = [y[1] for y in all_nodes]

        edge_x = []
        edge_y = []

        for k in self.links.keys():
          for v in self.links[k]:
            edge_x += [v[0], k[0], None]
            edge_y += [v[1], k[1], None]

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

        self.figure = go.FigureWidget(data=[edge_trace, node_trace],
                     layout=go.Layout(
                        titlefont_size=16,
                        showlegend=False,
                        margin=dict(b=20,l=5,r=5,t=40),
                        xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                        yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                        )

        self.num_nodes = len(all_nodes)
        self.colours = self.figure.data[1].marker.color

        return self.figure


    def highlight_route(self, node):
        route_x = []
        route_y = []

        def get_route_coords(node):
            for n in node:
                if self.links.get(n) is not None:
                    for v in self.links.get(n):
                        route_x.append([n[0], v[0], None])
                        route_y.append([n[1], v[1], None])
                        get_route_coords([v])

        get_route_coords(node)

        route_x = sum(route_x, [])
        route_y = sum(route_y, [])

        return route_x, route_y


    def find_journey(self, x, y):
        route_x, route_y = self.highlight_route([(x, y)])
        return self.figure.add_trace(go.Scatter(
        x=route_x, y=route_y,
        line=dict(width=1, color='red'),
        hoverinfo='none',
        mode='lines'))


    def reset_fig(self):
        self.figure.data[1].marker.color = ['green'] * self.num_nodes
        self.figure.data[1].marker.opacity = [1] * self.num_nodes
        self.figure.data = self.figure.data[0:2]


    def format_selection(self, selection):
        colours = ['green'] * self.num_nodes
        alphas = [0.2] * self.num_nodes

        colours[selection['pointIndex']] = 'blue'
        alphas[selection['pointIndex']] = 1

        self.figure.data[1].marker.color = colours
        self.figure.data[1].marker.opacity = alphas
