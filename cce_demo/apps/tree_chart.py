import math
import re

import plotly.graph_objects as go
import numpy as np
import pandas as pd

from apps.queries import criteria_tree_sql

pd.set_option('display.max_columns', None)

line_alpha = 0.5

default_axis_params = dict(showgrid=False,
                           zeroline=False,
                           showticklabels=False,
                           showline=False)

chart_margin = dict(b=0, l=5, r=5, t=0)
chart_height = 700


def calc_time(hours):
    days = int(hours / 24)
    hours = hours % 24
    return f'{days}d, {hours}h'

def get_all_nodes(data_frame):
    all_nodes_count = data_frame['Coordinates'].value_counts()
    all_nodes_actions = data_frame[['Coordinates', 'ACTION_TYPE_DESC', 'Stage']].set_index('Coordinates').drop_duplicates()
    all_nodes = pd.merge(all_nodes_count, all_nodes_actions, left_index=True, right_index=True)
    all_nodes.columns = ['count', 'label', 'stage']
    all_nodes = all_nodes.T.to_dict()
    all_nodes = {n: v for n, v in all_nodes.items() if all_nodes[n]['count'] > 0}
    return all_nodes


def group_customers(route_df):
    customer_dict = {}
    for ix, r in route_df.iterrows():
      for i, account in enumerate(r['Accounts']):
        customer_dict[(ix, account, r['Orders'][i])] = r['Devices'][i]
    return customer_dict


def get_routes(data_frame):
    cols_to_drop = ['Stage', 'Next_Stage', 'ACTION_TYPE_DESC', 'ORDER_CREATION_DATE']
    data_frame = data_frame.drop(cols_to_drop, 1).dropna()

    data_frame['Route'] = list(data_frame[['Coordinates', 'Coordinates_Next']].itertuples(index=False, name=None))

    routes = data_frame[['Route', 'Duration_Next', 'ACCOUNT_NO_ANON', 'ORDER_ID_ANON', 'MSISDN_ANON']].groupby('Route').agg({'Duration_Next': ['sum', 'count'], 'ACCOUNT_NO_ANON': list, 'ORDER_ID_ANON': list, 'MSISDN_ANON': list})
    routes.columns = [i + '_' + j for i, j in routes.columns]
    routes.columns = ['Duration', 'Count', 'Accounts', 'Orders', 'Devices']

    customers = group_customers(routes)
    routes = routes[['Duration', 'Count']]
    routes = routes.T.to_dict()

    for k, v in customers.items():
      if 'Customers' not in routes[k[0]].keys():
        routes[k[0]]['Customers'] = {(k[1], k[2]): v}
      else:
        routes[k[0]]['Customers'][(k[1], k[2])] = v

    return routes


def get_coordinates(data_frame):
    width_lookup = data_frame[['Stage', 'ACTION_TYPE_DESC']].drop_duplicates()

    stage_counts = width_lookup.groupby('Stage')['ACTION_TYPE_DESC'].count()

    width_lookup['Counter'] = range(len(width_lookup))
    width_lookup = width_lookup.set_index(['Stage', 'ACTION_TYPE_DESC'])
    width_lookup = width_lookup.groupby(axis=0, level=0).rank(axis=1, method='dense', ascending=True)
    width_lookup = width_lookup.sort_index()

    width = data_frame['Stage'].value_counts().max() * 0.75
    height = 10
    top = height * 0.9
    h_spacing = round(height / data_frame['Stage'].max() * 0.85, 2)

    node_number = 0
    coordinates = {}
    for _, row in data_frame[['ACTION_TYPE_DESC', 'Stage']].iterrows():

        current_action = row['ACTION_TYPE_DESC']
        current_stage = row['Stage']

        stage_count = stage_counts[current_stage]
        w_spacing = round(width / stage_count, 2)

        coordinates[node_number] = (w_spacing//2 + w_spacing * (width_lookup.loc[(current_stage, current_action), 'Counter'] - 1),
                                    round(top - h_spacing * current_stage, 2))
        node_number += 1

    return coordinates


def get_figure(df=None, service_types=None, customer_types=None,
               deals=None, action_status=None, start_date_val=None, end_date_val=None,
               dispute_val=None, action_filter=None, fault_filter=None,
               min_hours=None, has_action=None):
    """
    df:             provide a custom data frame for the function to work with.
                    Not used really
    service_types:  the type of service used for filtering. A tickbox with two
                    options for now but more may be needed
    customer_types: taken from the customers table, not all types seem to appear
                    in the orders table so be careful
    deals:          the deals of each order. These are actually grouped and each
                    value corresponds to multiple sub-deals usually meaning
                    different terms and pricing
    action_status:  action status represents the final status of the order
    start_date_val: the start value of the date picker
    end_date_val:   the end value of the date picker
    dispute_val:    whether or not the customer had a dispute
    action_filter:  the final action of that order
    fault_filter:   whether or not the customer lodged a fault
    min_hours:      the minimum nuber of journey hours
    has_action:     returns orders that, over time, have contained any of the
                    actions specified
    """

    hover_item_limit = 5

    if df is None:
        df = pd.io.gbq.read_gbq(criteria_tree_sql(service_types, customer_types,
                                                  deals, action_status, start_date_val,
                                                  end_date_val, dispute_val, action_filter,
                                                  fault_filter, min_hours, has_action),
                                project_id='bcx-insights',
                                dialect='standard')

    # If the query returns no results then display an empty chart
    if len(df) == 0:
        return go.FigureWidget({'data':
            {
                'x': [],
                'y': [],
                'mode': 'markers',
                'marker': {'size': 1}
            },
            'layout': go.Layout(
                xaxis=default_axis_params,
                yaxis=default_axis_params,
                height=chart_height
            )}), {}, {}

    df['ACTION_TYPE_DESC'] = df['ACTION_TYPE_DESC'].fillna('Other')

    coordinates = get_coordinates(df)
    coordinates_series = pd.Series(coordinates, name='Coordinates')

    df = df.join(coordinates_series)
    df = df.merge(df[['Stage', 'Coordinates', 'ORDER_ID_ANON', 'Duration']],
                  how='left',
                  left_on=['ORDER_ID_ANON', 'Next_Stage'],
                  right_on=['ORDER_ID_ANON', 'Stage'],
                  suffixes=('', '_Next'))
    df = df.drop(['Stage_Next', 'Duration'], axis=1).drop_duplicates()  # Keep Duration_Next

    routes = get_routes(df)
    all_nodes = get_all_nodes(df)

    links = df[['Coordinates', 'Coordinates_Next']].dropna().groupby('Coordinates')['Coordinates_Next'].apply(
        list).to_dict()

    colours = ['green'] * len([n for n in all_nodes.keys()])

    mean_count = np.mean([v['Count'] for v in routes.values()])
    mean_duration = np.mean([v['Duration'] for v in routes. values()])

    arrows = []

    for k, v in routes.items():

        route_relative_count = v['Count'] / mean_count
        route_relative_duration = v['Duration'] / mean_duration if mean_duration > 0 else 0

        r = int(240 * np.clip(math.tanh(route_relative_duration / 2), 0, 1))
        g = int(240 * (1 - np.clip(math.tanh(route_relative_duration / 2), 0, 1)))
        b = int(100 * (1 - np.clip(math.tanh(route_relative_duration / 2), 0, 1)) * 0.4)

        width = min((1.2 + route_relative_count) ** 3, 8)

        arrows.append(dict(
            ax=k[0][0],
            ay=k[0][1],
            axref='x',
            ayref='y',
            showarrow=True,
            arrowhead=2,
            arrowwidth=min(width, 6),
            arrowcolor=f'rgba({r}, {g}, {b}, {line_alpha})',
            x=k[1][0],
            y=k[1][1],
            xref='x',
            yref='y',
            standoff=15))

    newline_labels = [v['label'].replace(' ', '<br>') for v in all_nodes.values()]
    hover_labels = []
    text_positions = []

    node_df = df[['ACCOUNT_NO_ANON', 'ORDER_ID_ANON',
    'MSISDN_ANON', 'Coordinates']].drop_duplicates()

    node_x = []
    node_y = []

    for node, v in all_nodes.items():
        node_x.append(node[0])
        node_y.append(node[1])

        lab = v['label']

        if v['stage'] < df['Stage'].value_counts().index[-1]:
            text_positions.append('bottom center')
        else:
            text_positions.append('top center')

        total_count = all_nodes[node]['count']

        if total_count <= hover_item_limit and total_count > 0:

            node_data = node_df.loc[node_df['Coordinates'] == node, ['ACCOUNT_NO_ANON', 'ORDER_ID_ANON', 'MSISDN_ANON']]

            # We reset the index so that all accounts, devices and orders are grouped nicely
            # Devices will only be grouped nucely with orders if we add the third 'invisible' index
            node_data = node_data.set_index(['ACCOUNT_NO_ANON', 'ORDER_ID_ANON', [''] * len(node_data)])

            node_data = pd.Series(node_data.values.T[0], index=node_data.index)
            node_data = node_data.drop_duplicates().sort_index().to_string().replace('\n', '<br>')

            node_data = re.sub(r'(\s{8})\s+', ' ' * 41, node_data)
            node_data = node_data.replace('ACCOUNT_NO_ANON', 'Customer' + ' ' * 19)
            node_data = node_data.replace('ORDER_ID_ANON', 'Order ID' + ' ' * 30 + 'Device ID')
            hover_labels.append(f'{lab}<br>{node_data}')
        elif total_count > hover_item_limit:
            hover_labels.append(lab + f"<br>{str(all_nodes[node]['count'])}")

    traces = go.Scatter(
        x=node_x, y=node_y,
        mode='markers+text',
        textposition=text_positions,
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

    def get_route_coords(nodes):
        for n in nodes:
            n = tuple(n)
            if paths.get(n) is not None:
                for v in paths[n]:
                    route_x.append([n[0], v[0], None])
                    route_y.append([n[1], v[1], None])

                    get_route_coords([v])

    get_route_coords(node)

    route_x = sum(route_x, [])
    route_y = sum(route_y, [])

    return route_x, route_y


def find_journey(figure, paths, routes, x, y, hover_labels):
    route_x, route_y = highlight_route(paths, [(x, y)])

    link_coords = [v for v in zip(route_x, route_y) if v[0] is not None]
    route_coords = [(link_coords[i], link_coords[i + 1]) for i, _ in enumerate(link_coords) if i + 1 < len(link_coords)]

    if len(link_coords) == 0:
        link_coords = [(x, y)]

    if figure['data'][-1].get('marker') is None:
        figure['data'] = figure['data'][:-1]

    arrow_annotations = [a for a in figure['layout']['annotations'] if 'bordercolor' not in a.keys()]
    marker_colours = figure['data'][0]['marker']['color']

    coords = list(zip(figure['data'][0]['x'], figure['data'][0]['y']))
    selection_index = coords.index((x, y))

    if 'blue' not in marker_colours or (
            'blue' in marker_colours and figure['data'][0]['marker']['color'][selection_index] != 'blue'):
        colours = ['green'] * len(figure['data'][0]['marker']['color'])
        alphas = [0.1] * len(figure['data'][0]['x'])

        for c in link_coords:
            if c in coords:
                c_inx = coords.index(c)
                colours[c_inx] = 'orange'
                alphas[c_inx] = line_alpha

            colours[selection_index] = 'blue'
            alphas[selection_index] = 0.9

        filtered_hover_labels = []
        destination_nodes = [node[1] for node in route_coords]

        for node in route_coords:
            if node[1] in destination_nodes:
                for n in routes.keys():
                    if node == n:
                        node_df = pd.Series(routes[node]['Customers'])
                        filtered_hover_labels.append(node_df.to_string().replace('\n', '<br>'))
            else:
                filtered_hover_labels.append(hover_labels[node])

        print(filtered_hover_labels)

        figure['data'][0]['marker']['color'] = colours
        figure['data'][0]['marker']['opacity'] = alphas

        figure['layout']['annotations'] = arrow_annotations
        for i, line in enumerate(figure['layout']['annotations']):
            c = line['arrowcolor']
            figure['layout']['annotations'][i]['arrowcolor'] = c.replace(f'{line_alpha}', '0.1')

        figure = go.FigureWidget(data=figure)
        figure.add_trace(go.Scatter(
            x=route_x, y=route_y,
            line=dict(width=3, color=f'rgba(255, 0, 0, 0.9)'),
            hoverinfo='text',
            hovertext=filtered_hover_labels,
            mode='lines'))

        annotations = list(figure['layout']['annotations'])

        for t in routes.keys():
            if t in route_coords:
                customer_data = pd.Series(routes[t]['Customers'])
                customer_data = customer_data.reset_index()

                customers = customer_data['level_0'].drop_duplicates().tolist()
                orders = customer_data['level_1'].drop_duplicates().tolist()
                devices = customer_data[0].drop_duplicates().tolist()

                customer_counts = str(len(customers)) + ' customer'
                order_counts = str(len(orders)) + ' order'
                device_counts = str(len(devices)) + ' device'

                if len(customers) > 1:
                    customer_counts += 's'

                if len(orders) > 1:
                    order_counts += 's'

                if len(devices) > 1:
                    device_counts += 's'

                customer_hours = calc_time(int(routes[t]["Duration"]))

                if routes[t]["Duration"] > 1 or routes[t]["Duration"] == 0:
                    customer_hours += 's'

                annotations.append(
                    go.layout.Annotation(x=(t[0][0] + t[1][0]) / 2,
                                         y=(t[0][1] + t[1][1]) / 2,
                                         text=f'<b>{customer_counts}<br>{order_counts}<br>{device_counts}<br>{customer_hours}</b>',
                                         font={'size': 14},
                                         bgcolor='white',
                                         bordercolor='black'
                                         ))

        figure.update_layout(annotations=annotations)

    else:

        colours = ['green'] * len(figure['data'][0]['marker']['color'])
        alphas = [0.9] * len(figure['data'][0]['marker']['opacity'])

        figure['data'][0]['marker']['color'] = colours
        figure['data'][0]['marker']['opacity'] = alphas

        figure['layout']['annotations'] = arrow_annotations
        for i, line in enumerate(figure['layout']['annotations']):
            c = line['arrowcolor']
            figure['layout']['annotations'][i]['arrowcolor'] = c.replace('0.1', f'{line_alpha}')

        figure['data'][0]['hovertext'] = list(hover_labels.values())

    return figure


def default_chart():
    empty_scatter = {'data':
        {
            'x': [[]],
            'y': [[]],
            'mode': 'markers',
            'marker': {'size': 1}
        },
        'layout': go.Layout(
            height=chart_height,
            xaxis=default_axis_params,
            yaxis=default_axis_params
        )}

    return go.FigureWidget(data=empty_scatter,
                           layout=go.Layout(
                               titlefont_size=16,
                               showlegend=False,
                               height=chart_height,
                               xaxis=default_axis_params,
                               yaxis=default_axis_params)
                           )
