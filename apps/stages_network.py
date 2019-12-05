import dash
import dash_bootstrap_components as dbc
import dash_core_components as dcc
import dash_html_components as html
from dash.dependencies import Input, Output, State
from dash.exceptions import PreventUpdate
import time
import pandas as pd
from datetime import datetime as dt, timedelta

from app import app
import numpy as np
import networkx as nx
import matplotlib.pyplot as plt
import pandas_gbq
import plotly.graph_objs as go
import plotly as ply
#import chart_studio.plotly as py

#####################################################################################################################

#####################################################################################################################


def service_options():
    service_sql = """SELECT DISTINCT SERVICE_TYPE value FROM
    `bcx-insights.telkom_customerexperience.customerdata_20190902_00_anon`
    WHERE CUSTOMER_NO_ANON in
    (SELECT DISTINCT ACCOUNT_NO_ANON FROM
    `bcx-insights.telkom_customerexperience.orders_20190903_00_anon`)"""

    options = pd.io.gbq.read_gbq(service_sql,
                                    project_id='bcx-insights',
                                    dialect='standard').fillna('N/A')

    options['label'] = options['value'].str.title()

    options = [{'label': x['label'],
               'value': x['value']} for _, x in options.iterrows()]

    return options


service_type_DD = dcc.Dropdown(
    options=service_options(),
    value=[],
    multi=True,
    placeholder="Select a Service Type...",
    id = 'service_type_DD'
) 


def customer_type():
    type_sql = """SELECT DISTINCT CUSTOMER_TYPE_DESC value FROM
    `bcx-insights.telkom_customerexperience.customerdata_20190902_00_anon`
    WHERE CUSTOMER_NO_ANON in
    (SELECT DISTINCT ACCOUNT_NO_ANON FROM
    `bcx-insights.telkom_customerexperience.orders_20190903_00_anon`)
    ORDER BY CUSTOMER_TYPE_DESC"""

    types = pd.io.gbq.read_gbq(type_sql,
                                    project_id='bcx-insights',
                                    dialect='standard').fillna('N/A')

    types['label'] = types['value'].str.title()

    types = [{'label': x['label'],
                'value': x['value']} for _, x in types.iterrows()]

    return types

customer_type_DD = dcc.Dropdown(
    options=customer_type(),
    value=[],
    multi=True,
    placeholder="Select a Customer Type...",
    searchable = True,
    id = 'customer_type_DD'
) 

def action_type():
    type_sql = """SELECT distinct ACTION_TYPE_DESC
    FROM `bcx-insights.telkom_customerexperience.orders_20190926_00_anon`
    order by ACTION_TYPE_DESC"""

    df = pd.io.gbq.read_gbq(type_sql,
                            project_id='bcx-insights',
                            dialect='standard')

    df['ACTION_TYPE_DESC'] = df['ACTION_TYPE_DESC'].drop_duplicates()

    df = df['ACTION_TYPE_DESC'].dropna().tolist()

    options = [{'label': v, 'value': v} for v in df]

    return options

action_type_DD = dcc.Dropdown(
    options=action_type(),
    value=[],
    multi=True,
    placeholder="Select a Action Type...",
    searchable = True,
    id = 'action_type_DD'
) 






def deal_type():
    type_sql = """SELECT distinct DEAL_DESC
    FROM `bcx-insights.telkom_customerexperience.orders_20190926_00_anon`
    order by DEAL_DESC"""

    df = pd.io.gbq.read_gbq(type_sql,
                            project_id='bcx-insights',
                            dialect='standard')

    df['DEAL_DESC'] = df['DEAL_DESC'].drop_duplicates()

    df = df['DEAL_DESC'].dropna().tolist()

    options = [{'label': v, 'value': v} for v in df]

    return options

deal_type_DD = dcc.Dropdown(
    options=deal_type(),
    value=[],
    multi=True,
    placeholder="Select a Deal Type...",
    searchable = True,
    id = 'deal_type_DD'
) 







def has_dispute():
    return [{'label': 'Either', 'value': 'Either'},
            {'label': 'Yes', 'value': 'Yes'},
            {'label': 'No', 'value': 'No'}]

has_dispute_DD = dcc.Dropdown(
    options=has_dispute(),
    value=[],
    multi=True,
    placeholder="Has Disputes...",
    id = 'has_dispute_DD'
) 

def has_fault():
    return [{'label': 'Either', 'value': 'Either'},
            {'label': 'Yes', 'value': 'Yes'},
            {'label': 'No', 'value': 'No'}]

has_fault_DD = dcc.Dropdown(
    options=has_fault(),
    value=[],
    multi=True,
    placeholder="Has Faults...",
    id = 'has_fault_DD'
) 

def time_measure():
    return [{'label': 'days', 'value': 'days'},
            {'label': 'hours', 'value': 'hours'},
            {'label': 'minutes', 'value': 'minutes'},
            {'label': 'seconds', 'value': 'seconds'}]

total_journey_time_DD = dcc.RadioItems(
    options=time_measure(),
    value='days',
    id = 'total_journey_time_DD'
)

stages_date_picker = dcc.DatePickerRange(
        min_date_allowed=dt(1995, 8, 5),
        #max_date_allowed=dt(2017, 9, 19),
        #initial_visible_month=dt(2017, 8, 5),
        end_date=dt.now(),
        start_date = '2019-01-01',
        id = 'stages_date_picker'
    )


stages_slider = dcc.RangeSlider(
    marks={i: '{}'.format(i) for i in range(1,31)},
    min=1,
    max=30,
    value=[1, 30],
    id = 'stages_slider'
)  

journey_duration_min = dbc.Input(id="journey_duration_min", placeholder="Journey Duration Min...", type="number")
journey_duration_max = dbc.Input(id="journey_duration_max", placeholder="Journey Duration Max...", type="number")

list_filters =[dbc.Alert("Service Type (Customer Table)", color="secondary",style={'padding' : '5px 5px 5px 5px'}),
               service_type_DD,
               dbc.Alert("Customer Type (Customer Table)", color="secondary",style={'padding' : '5px 5px 5px 5px'}),
               customer_type_DD,
               dbc.Alert("Contains Action (Orders Table)", color="secondary",style={'padding' : '5px 5px 5px 5px'}),
               action_type_DD,
               dbc.Alert("Deal Type", color="secondary",style={'padding' : '5px 5px 5px 5px'}),
               deal_type_DD,
               dbc.Alert("Dispute Status", color="secondary",style={'padding' : '5px 5px 5px 5px'}),
               has_dispute_DD,
               dbc.Alert("Fault Status", color="secondary",style={'padding' : '5px 5px 5px 5px'}),
               has_fault_DD,
               dbc.Alert("Total Journey Time Units", color="secondary",style={'padding' : '5px 5px 5px 5px'}),  
               total_journey_time_DD,
               dbc.Alert("Total Journey Duration", color="secondary",style={'padding' : '5px 5px 5px 5px'}),
               journey_duration_min,
               journey_duration_max,
               dbc.Alert("Date Range", color="secondary",style={'padding' : '5px 5px 5px 5px'}),
               stages_date_picker,
               dbc.Alert("Final Stage Range", color="secondary",style={'padding' : '5px 5px 5px 5px'}),
               stages_slider,
               
               
               ]

run_button = dbc.Button("Run", color="secondary", className="mb-3", id='run_button',style={})

# Collapse Button (filters)
collapse = html.Div(
    [
        dbc.Button(style={'width': '200px','background-color': '#ffff','border-color': '#ffff'}),
        dbc.Button(
            "Filters",
            id="collapse-button_filters",
            className="mb-3",
            color="primary",
        ),
        run_button,
        dbc.Collapse(
            dbc.Card(dbc.CardBody(list_filters,style={'width': '95%','display': 'inline-block'})),
            id="collapse_stages",
        )
    ],
        
)

#####################################################################################################################
##Graph

data_trace = []

fig = go.Figure(data=data_trace,
             layout=go.Layout(
                title='',
                titlefont=dict(size=16),
                showlegend=False,
                hovermode='closest',
                margin=dict(b=20,l=5,r=5,t=40),
                annotations=[ dict(
                    text="",
                    showarrow=False,
                    xref="paper", yref="paper",
                    x=0.005, y=-0.002 ) ],
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))
                
render_graph = html.Div([
    html.Div([html.H1("Aggregated Order and Device Journey")], className="row", style={'textAlign': "center"}),
    html.Div([dcc.Graph(id="stages-graph", figure = fig, style={'float':'left','width': '1300px','height' : '700px'})],id = 'graph-Div'),
], className="container")



#####################################################################################################################
layout = html.Div([html.Div([collapse]),
                       render_graph,
                       html.Div(id = 'test-p')])

#####################################################################################################################
# Collapse Filter components
@app.callback(
    Output("collapse_stages", "is_open"),
    [Input("collapse-button_filters", "n_clicks")],
    [State("collapse_stages", "is_open")],
)
def toggle_collapse(n, is_open):
    if n:
        return not is_open
    return is_open


## Filters call back

## Filter functions for callback
def build_query(iterable, field_name):
    if iterable is not None:
        if len(iterable) > 0:
            iterable = ','.join(["'" + s + "'" for s in iterable])
            iterable = 'and {} IN ({})'.format(field_name, iterable)
            return iterable
        else:
            return ''
    else:
        return ''
    
def dispute_query(dispute_val, start_date_val, end_date_val):
    sql = """
        (SELECT DISTINCT ACCOUNT_NO_ANON dispute_id FROM
        `bcx-insights.telkom_customerexperience.disputes_20190903_00_anon`
        WHERE RESOLUTION_DATE BETWEEN '{}' AND '{}') as disputes
        on orders.ACCOUNT_NO_ANON = disputes.dispute_id""".format(start_date_val, end_date_val)

    if dispute_val == 'Yes':
        join_type = 'JOIN '
        return join_type + sql, ''
    elif dispute_val == 'No':
        join_type = 'LEFT JOIN '

        return join_type + sql, "AND dispute_id is Null"
    else:
        return '', ''
    
def fault_query(fault_val, start_date_val, end_date_val):
    sql = """
        (SELECT DISTINCT SERVICE_KEY_ANON fault_id FROM
        `bcx-insights.telkom_customerexperience.faults_20190903_00_anon`
        WHERE DATDRGT BETWEEN '{}' AND '{}') as faults
        on orders.ACCOUNT_NO_ANON = faults.fault_id""".format(start_date_val, end_date_val)

    if fault_val == 'Yes':
        join_type = 'JOIN '
        return join_type + sql, ''
    elif fault_val == 'No':
        join_type = 'LEFT JOIN '

        return join_type + sql, "AND fault_id is Null"
    else:
        return '', ''
    
def between_date_query(start_date_val, end_date_val):
    date_range = """AND orders.ORDER_CREATION_DATE BETWEEN '{}' AND '{}'""".format(start_date_val, end_date_val)

    return "{}".format(date_range)

def sql_query_call(service_type, 
                   customer_type, 
                   deal_desc, 
                   action_status,
                   start_date_val, 
                   end_date_val, 
                   dispute_val,
                   fault_val,
                   #action_filter,
                   #min_hours, 
                   #has_action
                  ):
    
    service_type = build_query(service_type, 'SERVICE_TYPE')
    customer_type = build_query(customer_type, 'CUSTOMER_TYPE_DESC')
    deal_desc = build_query(deal_desc, 'DEAL_DESC')                           #add add hoc deal
    #deal_type = "'TBiz BB Capped Advanced'"
    #has_action = includes_action(has_action, start_date_val, end_date_val)

    dispute_join, dispute_where = dispute_query(dispute_val, start_date_val, end_date_val)
    fault_join, fault_where = fault_query(fault_val, start_date_val, end_date_val)
    #hours_sql_field, hours_where = build_min_hours(min_hours)
    #action_status_subquery, action_status_where = last_status_or_action_query(action_status, action_filter)

    if start_date_val is not None and end_date_val is not None:
        date_range = between_date_query(start_date_val, end_date_val)
    else:
        date_range = ''

        
    query = """
    SELECT *
    FROM `bcx-insights.telkom_customerexperience.orders_20190926_00_anon` as orders
    LEFT JOIN
    (SELECT DISTINCT CUSTOMER_NO_ANON, SERVICE_TYPE, CUSTOMER_TYPE_DESC FROM
    `bcx-insights.telkom_customerexperience.customerdata_20190902_00_anon`) as custs
    ON custs.CUSTOMER_NO_ANON = orders.ACCOUNT_NO_ANON
    WHERE 1=1
    AND orders.SOURCE = "F"
    {}
    {}
    {}
    {}
    {}
    {}
    ORDER BY ORDER_CREATION_DATE, ACTION_CREATION_DATE, ORDER_ID_ANON
    """.format(deal_desc,customer_type, service_type, dispute_where, fault_where, date_range)
    
    return query

def between_date_query2(time_filter_min, time_filter_max, time_measure, min_total_journey_time, max_total_journey_time):
    
    if time_filter_max is None:
        time_filter_max = max_total_journey_time
        
    if time_filter_min is None:
        time_filter_min = min_total_journey_time
        
    if time_measure == 'minutes':
        multiplier = 60
        
    elif time_measure == 'hours':
        multiplier = 60*60
        
    elif (time_measure == 'days' or time_measure is None): #default is days
        multiplier = 60*60*24
        time_measue = 'days'
        
    else:
        multiplier = 1
    
    min_val = time_filter_min*multiplier
    max_val = time_filter_max*multiplier
        
        
    return min_val, max_val, time_measure, multiplier



@app.callback(Output('stages-graph','figure'),
              [Input('run_button', 'n_clicks'),
               Input('service_type_DD', 'value'),
               Input('customer_type_DD', 'value'),
               Input('action_type_DD', 'value'),
               Input('has_dispute_DD', 'value'),
               Input('has_fault_DD', 'value'),
               Input('total_journey_time_DD', 'value'),
               Input('stages_date_picker', 'start_date'),
               Input('stages_date_picker', 'end_date'),
               Input('stages_slider', 'value'),
               Input("journey_duration_min", 'value'),
               Input("journey_duration_max", 'value'),
               Input('deal_type_DD', 'value')
               
               ])
def render_stages_graph(n_clicks,
                        service_type_DD,
                        customer_type_DD,
                        action_type_DD,
                        has_dispute_DD,
                        has_fault_DD,
                        total_journey_time_DD,
                        start_date,
                        end_date,
                        stages_slider,
                        journey_duration_min,
                        journey_duration_max,
                        deal_type_DD
                        ):
    if n_clicks >= 1:
        
        print(type(n_clicks) , n_clicks)
        print(type(service_type_DD), service_type_DD)
        print(type(customer_type_DD),customer_type_DD)
        print(type(action_type_DD),action_type_DD)
        print(type(has_dispute_DD),has_dispute_DD)
        print(type(has_fault_DD),has_dispute_DD)
        print(type(total_journey_time_DD),total_journey_time_DD)
        print(type(start_date),start_date)
        print(type(end_date),end_date)
        print(type(stages_slider),stages_slider)
        print(type(journey_duration_min),journey_duration_min)
        print(type(journey_duration_max),journey_duration_max)
        
        
        if service_type_DD == []:
            service_type_DD = None
            
        if customer_type_DD == []:
            customer_type_DD= None
        
        if action_type_DD == []:
            action_type_DD= None
            
        if has_dispute_DD == []:
            has_dispute_DD= None
            
        if has_fault_DD == []:
            has_fault_DD= None
        
        if deal_type_DD == []:
            deal_type_DD = None
        
        #start_date = start_date[:10]
        end_date = end_date[:10]
        deal_desc = deal_type_DD# ['TBiz BB Capped Advanced']
        
        
        print(type(n_clicks) , n_clicks)
        print(type(service_type_DD), service_type_DD)
        print(type(customer_type_DD),customer_type_DD)
        print(type(action_type_DD),action_type_DD)
        print(type(has_dispute_DD),has_dispute_DD)
        print(type(has_fault_DD),has_dispute_DD)
        print(type(total_journey_time_DD),total_journey_time_DD)
        print(type(start_date),start_date)
        print(type(end_date),end_date)
        print(type(stages_slider),stages_slider)
        print(type(journey_duration_min),journey_duration_min)
        print(type(journey_duration_max),journey_duration_max)
        
        
        
        query_called = sql_query_call(service_type_DD,
                       customer_type_DD,
                       deal_desc,
                       0,
                       start_date,
                       end_date,
                       has_dispute_DD,
                       has_fault_DD
                       )
        #create order dataframe
        print(query_called)
        orders_df = pd.read_gbq(query_called,
                project_id = 'bcx-insights',
                dialect = 'standard').fillna('N/A')
        orders_df = orders_df.drop_duplicates()
        orders_df['MSISDN_ANON'] = orders_df['MSISDN_ANON'].astype(str)
        orders_df['ORDER_ID_ANON'] = orders_df['ORDER_ID_ANON'].astype(str)
        unique_col = 'combo'
        orders_df[unique_col] = orders_df['MSISDN_ANON']+'_'+orders_df['ORDER_ID_ANON']
        orders_df['stage'] = orders_df.groupby(unique_col).cumcount()+1
        final_stage = orders_df[[unique_col,'stage', 'ORDER_CREATION_DATE']].groupby(unique_col).agg({'stage': 'max', 'ORDER_CREATION_DATE': ['min', 'max']})
        final_stage.columns = ["_".join(pair) for pair in final_stage.columns]
        final_stage.rename(columns={'stage_max':'final_stage', 'ORDER_CREATION_DATE_min':'first_date', 'ORDER_CREATION_DATE_max':'last_date'}, inplace=True)
        
        orders_df = pd.merge(orders_df, final_stage, on=unique_col, how='outer')
        orders_df['accumulated_time'] = orders_df['ORDER_CREATION_DATE']-orders_df['first_date'] 
        orders_df['total_journey_time'] = orders_df['last_date']-orders_df['first_date'] 
        orders_df['accumulated_time'] = orders_df['accumulated_time'].dt.total_seconds()
        orders_df['total_journey_time'] = orders_df['total_journey_time'].dt.total_seconds()
        
        print(orders_df)
        
        number_of_stages_min = max(min(orders_df['final_stage']),stages_slider[0])
        number_of_stages_max = min(max(orders_df['final_stage']),stages_slider[1])
        
        orders_df = orders_df[(orders_df['final_stage'] >= number_of_stages_min) & (orders_df['final_stage'] <= number_of_stages_max)]
        
        time_filter_min = journey_duration_min
        time_filter_max = journey_duration_max
        time_measure = total_journey_time_DD
        
        min_total_journey_time = min(orders_df['total_journey_time'])
        max_total_journey_time = max(orders_df['total_journey_time'])
        
        min_time_val, max_time_val, time_measure, time_multiplier =  between_date_query2(time_filter_min, time_filter_max, total_journey_time_DD, min_total_journey_time, max_total_journey_time )
        
        orders_df = orders_df[(orders_df['total_journey_time'] >= min_time_val) & (orders_df['total_journey_time'] <= max_time_val)]
        
        
        
        
        if action_type_DD is None:
            contains_action = orders_df['ACTION_TYPE_DESC'].unique()
        else:
            contains_action = action_type_DD
        
        orders_df['match_action'] = orders_df['ACTION_TYPE_DESC'].isin(contains_action)
        
        match_max = orders_df[[unique_col,'match_action']].groupby(unique_col)['match_action'].max().reset_index()
        
        match_max.rename(columns={'match_action':'contains'}, inplace=True)
        orders_df = pd.merge(orders_df, match_max, on=unique_col, how='outer')
        orders_df = orders_df[orders_df['contains']==True]
        orders_df = orders_df.drop(columns=['match_action','contains'])
        orders_df['stage'] = orders_df['stage'].astype(str)
        orders_df['ActionType_OrderStatus'] = orders_df['ACTION_TYPE_DESC'] +' * '+ orders_df['ORDER_STATUS_DESC'] 
        
        type_status = list(sorted(orders_df['ActionType_OrderStatus'].unique()))
        
        max_string_len = 0#len(max(type_status, key=len))
        
        orders_df['stage_ActionType_OrderStatus'] = orders_df['stage'] +') * '+ orders_df['ACTION_TYPE_DESC'] +' * '+ orders_df['ORDER_STATUS_DESC'] 
        
        customers = list(orders_df[unique_col].unique())
        
        sorted_df = orders_df.sort_values('stage', ascending=False)
        
        
        print('Just before NX')
        print(orders_df.head())
        
        
        ## Create Graph Components (NetworkX)
        J = nx.Graph()
        J.clear()
        max_stage= 10 #max(orders_df['stage'].astype(int))
        actions = list(orders_df['stage_ActionType_OrderStatus'].unique())
        
        for i in range(1,max_stage+1):
  
            stage_actions = list(orders_df[orders_df['stage']==str(i)]['stage_ActionType_OrderStatus'].unique())
            #print(stage_actions)
            for k in range(len(stage_actions)):
                label = stage_actions[k]
                #print(label)
                #print(len(orders_df[orders_df['stage_ActionType_OrderStatus']==label]))
                label = label.split('* ',1)
                label = label[1]
                #print(label)
                height =  type_status.index(label) +1
                #print(height)
                #print(height)
                J.add_node(stage_actions[k],posi=((max_string_len*0.01)+(max_stage*0.01)+(i*1.25),height), acc_time = 0, node_count = len(orders_df[orders_df['stage_ActionType_OrderStatus']==stage_actions[k]]))
                #print(stage_actions[k])
                #print((i,k+1))
        
        #print('J Nodes')
        #print(J.nodes)
        
        #Create label nodes
        L = nx.Graph()
        
        for l in range(len(type_status)):
            label_start = type_status[l]
            height =  type_status.index(label_start) +1
            L.add_node(label_start,posi=(0.15,height))
            #print(label_start)
            
        # Draw the resulting graph
        pos = nx.get_node_attributes(J,'posi')
        nx.draw(J, pos ,with_labels=True, font_weight='bold', font_size = 7)
        
        # clear edges
        J.remove_edges_from(list(J.edges()))
        
        
        #create edges with data (count, days, ave_days)
        # accumulated time for nodes

        date_col = orders_df.columns.get_loc('ORDER_CREATION_DATE')
        acc_col = orders_df.columns.get_loc('accumulated_time')
        kpi = 5*((60*60*24)/time_multiplier)

        for c in customers:

            temp_df = orders_df[orders_df[unique_col] == c]
  
            for i in range(len(temp_df)-1):
                #print(i, c)
                if J.has_edge(temp_df.iloc[i, -1], temp_df.iloc[(i+1),-1]) == False:
                    #print(temp_df.iloc[i, -1], temp_df.iloc[(i+1),-1])
                    #print(temp_df.iloc[i,-4], temp_df.iloc[(i+1),-4])
                    J.add_edge(temp_df.iloc[i, -1], temp_df.iloc[(i+1),-1])
                    J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['count'] = 1
                    difference = (temp_df.iloc[(i+1),date_col]-temp_df.iloc[i, date_col])
                    J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['days'] = round(difference.total_seconds()/(time_multiplier),1)
                    J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['ave_days'] = round(J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['days']/ J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['count'],1)
            
                    acc_current = J.nodes[temp_df.iloc[(i+1),-1]]['acc_time']
                    J.nodes[temp_df.iloc[(i+1),-1]]['acc_time'] = round(acc_current + temp_df.iloc[(i+1),acc_col]/(time_multiplier),1) #acc_col is accumulated_time
                    #J.nodes[temp_df.iloc[(i+1),-1]]['node_count'] += 1
            
            
                    if J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['ave_days'] > kpi:
                        J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['color'] = 'red'
                    else:
                        J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['color'] = 'green'

                else:
                    current_count = J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['count']
                    J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['count'] = current_count + 1

                    current_days =  J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['days']
                    difference = (temp_df.iloc[(i+1),date_col]-temp_df.iloc[i, date_col])
                    J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['days'] = round(current_days + difference.total_seconds()/(time_multiplier),1)

                    J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['ave_days'] = round(J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['days']/ J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['count'],1)
            
                    acc_current = J.nodes[temp_df.iloc[(i+1),-1]]['acc_time']
                    J.nodes[temp_df.iloc[(i+1),-1]]['acc_time'] = round(acc_current + temp_df.iloc[(i+1),acc_col]/(time_multiplier),1) #acc_col is accumulated_time         
                    #J.nodes[temp_df.iloc[(i+1),-1]]['node_count'] += 1
            
                    if J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['ave_days'] > kpi:
                        J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['color'] = 'red'
                    else:
                        J.edges[temp_df.iloc[i, -1],temp_df.iloc[(i+1),-1]]['color'] = 'green'

        
        
        
        
        for j in J.nodes:
            #print(j)
            #print(J.nodes[j]['acc_time'])
            #print(J.nodes[j]['node_count'])
            J.nodes[j]['ave_journey'] = round(J.nodes[j]['acc_time']/J.nodes[j]['node_count'],1)
        
        
        # add edge weight attribute

        all_counts = []

        #Iterate through the graph edges to gather all the weights
        for (node1,node2,data) in J.edges(data=True):
            all_counts.append(data['count']) 

    
        total_count = sum(all_counts)
        #print('Total Counts',total_count)

        if total_count > 0:
            max_count = max(all_counts)
            min_count = min(all_counts)
            ave_count = total_count/len(all_counts)
            scale = 10

            #print('Total Counts',total_count)
            #print('Max Count',max_count)
            #print('Min Count', min_count)
            #print('Ave Count',ave_count)


            if (max_count-min_count) == 0:
                for (node1,node2,data) in J.edges(data=True):
                    J.edges[node1,node2]['weight'] = round(J.edges[node1,node2]['count'],1)

            else:                                                
                for (node1,node2,data) in J.edges(data=True):
                    J.edges[node1,node2]['weight'] = round(((J.edges[node1,node2]['count']-min_count)/(max_count-min_count))*scale+1,1)
    
        
        
        #Create Edges hover text
        def edge_hover(x, y, hover_txt, color):
            """
            Args:
                x: a tuple of the x from and to, in the form: tuple([x0, x1, None])
                y: a tuple of the y from and to, in the form: tuple([y0, y1, None])
                width: The width of the line

            Returns:
                a Scatter plot which represents a line between the two points given. 
            """
            return  go.Scatter(
                        x=x,
                        y=y,
                        text = hover_txt,
                        mode='markers',
                        hoverinfo='text',
                        hoverlabel = dict(
                                    bgcolor = color,
                                    font =  dict(color = 'white')
                                    ),
                        marker=go.Marker(opacity=0.1, symbol = 'triangle-left')
                        )
        
        
        #Create Edges
        def make_edge(x, y, width, color):
            """
            Args:
                x: a tuple of the x from and to, in the form: tuple([x0, x1, None])
                y: a tuple of the y from and to, in the form: tuple([y0, y1, None])
                width: The width of the line

            Returns:
                a Scatter plot which represents a line between the two points given. 
            """
            return  go.Scatter(
                        x=x,
                        y=y,
                        text = hover_txt,
                        line=dict(width=width,color=color),
                        hoverinfo=None,
                        mode='lines')
        
        data_trace = []
        
        p_0=5
        p_1=4
        
        for edge in J.edges(data=True):
            x0, y0 = J.nodes[edge[0]]['posi']
            x1, y1 = J.nodes[edge[1]]['posi']
    
            x=tuple([x0, x1, None])
            y=tuple([y0, y1, None])
    
            xh = tuple([(x0/(p_0+p_1)*p_0+x1/(p_0+p_1)*p_1),None])
            yh = tuple([(y0/(p_0+p_1)*p_0+y1/(p_0+p_1)*p_1),None])
    
            #print(edge)
    
            width = edge[2]['weight']
            color = edge[2]['color']
            hover_txt = 'Count: '+ str(round(edge[2]['count'],1)) + '<br />Ave '+time_measure+': ' + str(round(edge[2]['days']/edge[2]['count'],1))
    
            #print(width)
    
            data_trace.append(make_edge(x,y,width,color))
            #if else
            data_trace.append(edge_hover(xh,yh,hover_txt,color))
        
        #print(data_trace)
        
        node_trace = go.Scatter(
            x=[],
            y=[],
            text=[],
            mode='markers',
            hoverinfo='text',
            marker=dict(
            showscale=True,
                # colorscale options
                #'Greys' | 'YlGnBu' | 'Greens' | 'YlOrRd' | 'Bluered' | 'RdBu' |
                #'Reds' | 'Blues' | 'Picnic' | 'Rainbow' | 'Portland' | 'Jet' |
                #'Hot' | 'Blackbody' | 'Earth' | 'Electric' | 'Viridis' |
            colorscale='Blackbody',
            reversescale=True,
            color=[],
            size=10,
            colorbar=dict(
            thickness=15,
            title='No. of Journeys',
            xanchor='left',
            titleside='right'
        ),
        line=dict(width=2)))

        for node in J.nodes():
            x, y = J.nodes[node]['posi']
            node_trace['x'] += tuple([x])
            node_trace['y'] += tuple([y])
        
        
        for node in J.nodes(data=True):
            #print(node)
            node_trace['marker']['color']+=tuple([node[1]['node_count']])
            node_info = node[0]+ '<br />Journeys: ' +str(node[1]['node_count']) + '<br />Ave journey '+ time_measure + ': ' +str(round(node[1]['acc_time']/node[1]['node_count'],1))
            node_trace['text']+=tuple([node_info])
        
        
        data_trace.append(node_trace)
        #print(data_trace)
        label_size = max(round(16-(len(type_status)/6),0),4)
        
        label_trace = go.Scatter(
            x=[],
            y=[],
            text=[],
            mode='text',
            hoverinfo='none',
            opacity=0.8,
            textposition='middle right',
            textfont=dict( size = label_size,
                        color='#888'
                        )
            )

        for label in L.nodes():
            x, y = L.nodes[label]['posi']
            #print(L.nodes[label]['posi'])
            label_trace['x'] += tuple([x])
            label_trace['y'] += tuple([y])
            label_trace['text']+= tuple([label])
        
        
        
        data_trace.append(label_trace)
        
        
        return go.Figure(data=data_trace,
                layout=go.Layout(
                title='Date Range : ' + str(start_date) + ' -> ' + str(end_date),
                titlefont=dict(size=16),
                showlegend=False,
                hovermode='closest',
                margin=dict(b=20,l=5,r=5,t=40),
                annotations=[ dict(
                    text="",
                    showarrow=False,
                    xref="paper", yref="paper",
                    x=0.005, y=-0.002 ) ],
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False)))
    
    else:
        raise PreventUpdate
         
if __name__ == '__main__':
    app.run_server(debug=False) 
