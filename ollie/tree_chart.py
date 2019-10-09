
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import numpy as np
import pandas as pd

from queries import criteria_tree_sql

default_axis_params = dict(showgrid=False,
                        zeroline=False,
                        showticklabels=False,
                        showline=False)

chart_margin = dict(b=0,l=5,r=5,t=0)
chart_height = 700

def get_figure(df=None, service_types=None, customer_types=None,
                deals=None, action_status=None, date_val=None):

    num_nodes = 0
    links = {}
    labels = {}
    coords = {}
    counts = {}
    coords_map = {}

    if df is None:
        df = pd.io.gbq.read_gbq(criteria_tree_sql(service_types, customer_types,
                                            deals, action_status, date_val),
                                            project_id='bcx-insights',
                                            dialect='standard')

    if len(df) == 0:
        return go.FigureWidget({'data':
                    {
                    'x': [],
                    'y': [],
                    'mode': 'markers',
                    'marker': {'size': 1}
                    },
                    'layout':go.Layout(
                    xaxis=default_axis_params,
                    yaxis=default_axis_params,
                    height=chart_height
                    )}), links, {}

    df['ACTION_TYPE_DESC'] = df['ACTION_TYPE_DESC'].fillna('Other')

    df['Duration'] = (df['ORDER_CREATION_DATE'].diff().dt.days > 0) * df['ORDER_CREATION_DATE'].diff().dt.seconds/60

    WIDTH = df['Stage'].value_counts().max() * 0.75
    HEIGHT = 10

    TOP = HEIGHT * 0.9
    LEVEL_HEIGHT = HEIGHT // len(df)

    w_spacing = (WIDTH/df[['ACTION_TYPE_DESC', 'Stage']].drop_duplicates()['Stage'].value_counts()).round(2).to_dict()
    h_spacing = round(HEIGHT/(df['Stage'].max()), 2) * 0.85

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

    df = df[['ORDER_ID_ANON', 'MSISDN_ANON', 'Stage', 'Position', 'Duration']].groupby(['ORDER_ID_ANON', 'MSISDN_ANON', 'Stage']).agg({'Position': 'first', 'Duration': 'mean'})
    df['Link'] = df['Position'].shift(-1)

    times = df.groupby(['Position', 'Link']).mean().dropna()
    times = times.round(0).astype(int).to_dict()['Duration']

    max_stage_count = []

    for ix, l in df.iterrows():
      if type(l['Link']) is tuple and l['Position'][1] > l['Link'][1]:
        if l['Position'] not in links.keys():
          links[l['Position']] = {'joins':[], 'stage': ix[2]}

          max_stage_count.append(ix[2])

        if l['Link'] not in links[l['Position']]['joins']:
          links[l['Position']]['joins'].append(l['Link'])

    max_stage_count = len([s for s in max_stage_count if s == max(max_stage_count)])

    colours = ['green'] * len(all_nodes)

    node_x = [x[0] for x in all_nodes]
    node_y = [y[1] for y in all_nodes]

    edge_x = []
    edge_y = []

    for k in links.keys():
      for v in links[k]['joins']:
        edge_x += [v[0], k[0], None]
        edge_y += [v[1], k[1], None]

    edge_trace = go.Scatter(
        x=edge_x, y=edge_y,
        line=dict(width=0.5, color='rgba(0, 0, 0, 0.8)'),
        hoverinfo='none',
        mode='lines')

    labels = [v.replace(' ', '<br>') for v in labels.values()]
    hover_labels = []


    for i, node in enumerate(all_nodes):

        if counts[i] <=5:
            node_df = df[df['Position'] == node]
            ids = [str(id) for id in node_df.index.get_level_values(0)]
            devices = [str(dev) for dev in node_df.index.get_level_values(1)]

            hover_labels.append('ID-Device<br>' + '<br>'.join([f'{i[0]} - {i[1]}' for i in zip(ids, devices)]))
        else:
            hover_labels.append(labels[i].replace('<br>', ' ') + f'<br>{str(counts[i])}')

    node_trace = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        textposition=['bottom center'] * (len(labels) - max_stage_count) + ['top center'] * max_stage_count,
        text = labels,
        hoverinfo='text',
        hovertext=hover_labels,
        marker=dict(
            color=colours,
            size=10,
            line_width=2))

    figure = go.FigureWidget(data=[edge_trace, node_trace],
                 layout=go.Layout(
                    titlefont_size=16,
                    showlegend=False,
                    margin=chart_margin,
                    height=chart_height,
                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False))
                    )

    return figure, links, times


def highlight_route(paths, node):
    route_x = []
    route_y = []

    def get_route_coords(node):
        for n in node:
            n = tuple(n)
            if paths.get(n) is not None:
                for v in paths[n]['joins']:

                    route_x.append([n[0], v[0], None])
                    route_y.append([n[1], v[1], None])

                    get_route_coords([v])

    get_route_coords(node)

    route_x = sum(route_x, [])
    route_y = sum(route_y, [])

    return route_x, route_y


def find_journey(figure, paths, times, x, y):

    route_x, route_y = highlight_route(paths, [(x, y)])
    figure = go.FigureWidget(data=figure)

    link_coords = [v for v in zip(route_x, route_y) if v[0] is not None]
    route_coords = [(link_coords[i], link_coords[i+1]) for i, _ in enumerate(link_coords) if i + 1 < len(link_coords)]
    if len(route_coords) == 0:
        route_coords = [(x, y)]

    colours = ['green'] * len(figure['data'][1]['x'])

    coords = list(zip(figure['data'][1]['x'], figure['data'][1]['y']))

    selection_index = coords.index((x, y))

    if figure['data'][1]['marker']['color'][selection_index] != 'blue':
        alphas = [0.1] * len(figure['data'][1]['x'])
        for c in link_coords:
            if c in coords:
                c_inx = coords.index(c)
                colours[c_inx] = 'blue'
                alphas[c_inx] = 0.9

        figure.data[0].line.color = 'rgba(0, 0, 0, 0.3)'

        figure.add_trace(go.Scatter(
        x=route_x, y=route_y,
        line=dict(width=1.5, color='rgba(255, 0, 0, 0.9)'),
        hoverinfo='none',
        mode='lines'))

        annotations = []
        for t in times.keys():
            if t in route_coords:
                annotations.append(
                go.layout.Annotation(x = (t[0][0] + t[1][0])/2,
                                    y = (t[0][1] + t[1][1])/2,
                                    text = f'<b>{str(times[t])} hours</b>',
                                    font=dict(
                                    color="black",
                                    size=12)
                                    ))
        figure.update_layout(annotations=annotations)

    else:
        alphas = [0.9] * len(figure['data'][1]['x'])
        figure['data'] = figure['data'][0:2]
        figure.data[0].line.color = 'rgba(255, 255, 255, 0.8)'

    figure['data'][1]['marker']['color'] = colours
    figure['data'][1]['marker']['opacity'] = alphas

    return figure


def reset_fig(figure):
    num_nodes = len(figure['data'][1]['marker']['color'])
    figure['data'][1]['marker']['color'] = ['green'] * num_nodes
    figure['data'][1]['marker']['opacity'] = [1] * num_nodes
    figure['data'][0]['line']['color'] = 'rgba(0, 0, 0, 0.8)'
    figure['data'] = figure['data'][0:2]

    figure['layout']['annotations'] = []
    return figure


def default_chart():

    empty_scatter = {'data':
                {
                'x': [[]],
                'y': [[]],
                'mode': 'markers',
                'marker': {'size': 1}
                },
                'layout':go.Layout(
                height=chart_height,
                xaxis=default_axis_params,
                yaxis=default_axis_params
                )}

    return go.FigureWidget(data=empty_scatter,
                 layout=go.Layout(
                    titlefont_size=16,
                    showlegend=False,
                    height=chart_height,
                    margin=chart_margin,
                    xaxis=default_axis_params,
                    yaxis=default_axis_params)
                    )
