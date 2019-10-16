
import matplotlib.pyplot as plt
import plotly.graph_objects as go
import numpy as np
import pandas as pd

from queries import criteria_tree_sql

line_alpha = 0.5

default_axis_params = dict(showgrid=False,
                        zeroline=False,
                        showticklabels=False,
                        showline=False)

chart_margin = dict(b=0,l=5,r=5,t=0)
chart_height = 600

def get_figure(df=None, service_types=None, customer_types=None,
                deals=None, action_status=None, date_val=None,
                dispute_val=None, action_filter=None, fault_filter=None):

    num_nodes = 0
    hover_item_limit = 5
    all_nodes = {}
    links = {}
    coords = []

    if df is None:
        df = pd.io.gbq.read_gbq(criteria_tree_sql(service_types, customer_types,
                                            deals, action_status, date_val,
                                            dispute_val, action_filter,
                                            fault_filter),
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

    for i in range(df['Stage'].min(), df['Stage'].max() + 1):
        reasons = df.loc[df['Stage'] == i, 'ACTION_TYPE_DESC'].value_counts().to_dict()

        for j, r in enumerate(reasons.keys()):
            coords = (w_spacing[i] * (j + 0.5), TOP - i * h_spacing)
            all_nodes[coords] = {'label': r, 'stage': i}

    all_stages = [v['stage'] for v in all_nodes.values()]
    all_labels = [v['label'] for v in all_nodes.values()]

    coords_map = pd.Series(list(all_nodes.keys()), index=[all_stages, all_labels], name='Position')

    df = pd.merge(df, pd.Series(coords_map), how='left', left_on=['Stage', 'ACTION_TYPE_DESC'], right_index=True)

    df = df[['ACCOUNT_NO_ANON', 'ORDER_ID_ANON', 'MSISDN_ANON', 'Stage', 'Position', 'Duration']].groupby(['ACCOUNT_NO_ANON', 'ORDER_ID_ANON', 'MSISDN_ANON', 'Stage']).agg({'Position': 'first', 'Duration': 'mean'})
    df['Link'] = df['Position'].shift(-1)

    df = df.sort_index(level=['ACCOUNT_NO_ANON', 'ORDER_ID_ANON', 'MSISDN_ANON', 'Stage'],
    ascending=[False, True, True, True])

    counts = df['Position'].value_counts().to_dict()
    for k, v in counts.items():
        all_nodes[k]['count'] = v
    counts = None

    route_count = df[['Position', 'Link']]
    route_count['Count'] = 1
    route_count = route_count.groupby(['Position', 'Link']).sum().to_dict()['Count']

    times = df.groupby(['Position', 'Link']).mean().dropna()
    times = times.round(0).astype(int).to_dict()['Duration']


    route_count = {k: v for k, v in route_count.items() if k[0][1] > k[1][1]}
    times = {k: v for k, v in times.items() if k[0][1] > k[1][1]}

    routes = {k: {'Duration':times[k], 'Count':route_count[k]} for k in times.keys()}

    times = None
    route_count = None

    max_stage_count = []

    for ix, l in df.iterrows():
        if type(l['Link']) is tuple and l['Position'][1] > l['Link'][1]:
            if l['Position'] not in links.keys():
                links[l['Position']] = {'joins':[], 'stage': ix[3]}

            max_stage_count.append(ix[3])

            if l['Link'] not in links[l['Position']]['joins']:
                links[l['Position']]['joins'].append(l['Link'])

    max_stage_count = len([s for s in max_stage_count if s == max(max_stage_count)])

    colours = ['green'] * len(all_labels)

    node_x = [x[0] for x in all_nodes.keys()]
    node_y = [y[1] for y in all_nodes.keys()]

    edge_x = []
    edge_y = []

    arrows = []

    mean_count = np.mean([v['Count'] for v in routes.values()])

    for k, v in routes.items():

        width = v['Count']

        width = min((1.2 + width/mean_count)**3, 8)

        arrows.append(dict(
                    ax=k[0][0],
                    ay=k[0][1],
                    axref='x',
                    ayref='y',
                    showarrow=True,
                    arrowhead=2,
                    arrowwidth=min(width, 6),
                    arrowcolor=f'rgba(0, 0, 0, {line_alpha})',
                    x=k[1][0],
                    y=k[1][1],
                    xref='x',
                    yref='y',
                    standoff = 15))


    newline_labels = [v.replace(' ', '<br>') for v in all_labels]
    hover_labels = []

    for node, v in all_nodes.items():
        lab = v['label']
        if v['count'] <= hover_item_limit:
            node_df = df[df['Position'] == node]
            accs = [str(acc) for acc in node_df.index.get_level_values(0)]
            ids = [str(id) for id in node_df.index.get_level_values(1)]
            devices = [str(dev) for dev in node_df.index.get_level_values(2)]

            all_nodes[node]['accounts'] = accs

            hover_labels.append(f'{lab}<br>Account - ID - Device<br>' + '<br>'.join([f'{i[0]} - {i[1]} - {i[2]}' for i in zip(accs, ids, devices)]))
        else:
            hover_labels.append(lab + f"<br>{str(all_nodes[node]['count'])}")

    traces = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        textposition=['bottom center'] * (len(all_labels) - max_stage_count) + ['top center'] * max_stage_count,
        text = newline_labels,
        hoverinfo='text',
        hovertext=hover_labels,
        marker=dict(
            color=colours,
            size=25,
            line_width=2))

    figure = go.FigureWidget(data=traces,
                 layout=go.Layout(
                    titlefont_size=16,
                    showlegend=False,
                    margin=chart_margin,
                    height=chart_height,
                    xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                    annotations=arrows)
                    )

    return figure, links, routes


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


def find_journey(figure, paths, routes, x, y):

    route_x, route_y = highlight_route(paths, [(x, y)])

    link_coords = [v for v in zip(route_x, route_y) if v[0] is not None]
    route_coords = [(link_coords[i], link_coords[i+1]) for i, _ in enumerate(link_coords) if i + 1 < len(link_coords)]

    if len(link_coords) == 0:
        link_coords = [(x, y)]

    if figure['data'][-1].get('marker') is None:
        figure['data'] = figure['data'][:-1]

    coords = list(zip(figure['data'][0]['x'], figure['data'][0]['y']))
    selection_index = coords.index((x, y))

    if figure['data'][0]['marker']['color'][selection_index] != 'blue':
        colours = ['green'] * len(figure['data'][0]['marker']['color'])
        alphas = [0.1] * len(figure['data'][0]['x'])

        for c in link_coords:
            if c in coords:
                c_inx = coords.index(c)
                colours[c_inx] = 'orange'
                alphas[c_inx] = line_alpha

            colours[selection_index] = 'blue'
            alphas[selection_index] = 0.9

        figure['data'][0]['marker']['color'] = colours
        figure['data'][0]['marker']['opacity'] = alphas

        reset_annotaions = [a for a in figure['layout']['annotations'] if 'text' not in a.keys()]

        figure['layout']['annotations'] = reset_annotaions

        for line in figure['layout']['annotations']:
            line['arrowcolor'] = f'rgba(0, 0, 0, 0.2)'

        figure = go.FigureWidget(data=figure)
        figure.add_trace(go.Scatter(
        x=route_x, y=route_y,
        line=dict(width=3, color=f'rgba(255, 0, 0, 0.9)'),
        hoverinfo='none',
        mode='lines'))

        annotations = list(figure['layout']['annotations'])

        for t in routes.keys():
            if t in route_coords:
                customer_counts = f'{str(routes[t]["Count"])} order'
                if routes[t]["Count"] > 1 or routes[t]["Count"] == 0:
                    customer_counts += 's'

                annotations.append(
                go.layout.Annotation(x = (t[0][0] + t[1][0])/2,
                                    y = (t[0][1] + t[1][1])/2,
                                    text = f'<b>{customer_counts}<br>{str(routes[t]["Duration"])} hours</b>',
                                    font={'size':14},
                                    bgcolor='white',
                                    bordercolor='black'
                                    ))

        figure.update_layout(annotations=annotations)

    else:

        colours = ['green'] * len(figure['data'][0]['marker']['color'])
        alphas = [0.9] * len(figure['data'][0]['marker']['opacity'])

        figure['data'][0]['marker']['color'] = colours
        figure['data'][0]['marker']['opacity'] = alphas

        reset_annotaions = [a for a in figure['layout']['annotations'] if 'text' not in a.keys()]
        figure['layout']['annotations'] = reset_annotaions

        for line in figure['layout']['annotations']:
            line['arrowcolor'] = f'rgba(0, 0, 0, {line_alpha})'

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
