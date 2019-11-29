#All components needed to get the Dash server to run and to display our graphs in the Dash app
import dash
from dash.dependencies import Input, Output,State
import dash_core_components as dcc
import dash_html_components as html
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import plotly.graph_objects as go

#importing pandas and numpy for data manipulation and datetime to assist with the filtering of data.
from datetime import datetime as dt, timedelta
import pandas as pd
import numpy as np

from app import app

#Setting up the initial date that is used as parameters for the function to draw our initial graphs.
start_date = (dt.today() - timedelta(days=90)).date()
end_date = dt.today().date()

#Function that is used to generate the sunburst. It contains the Query that communicates with GBQ and then the data is passed to their respective graphs.
def generate_sunburst(start_date,end_date):
	Overall_Query = """with cte as (
	SELECT DISTINCT
	CASE WHEN orders.source = 'F' then 'Fixed' else 'Mobile' END as source,
	orders.ORIGINAL_SALES_CHANNEL_DESC,
	orders.ACCOUNT_NO_ANON, 
	orders.ORDER_CREATION_DATE as ORDER_DATE,
	if(disputes.ACCOUNT_NO_ANON is null, 'No Dispute', 'Dispute') has_dispute,
	CASE WHEN orders.OPEN_CLOSE_INDIC = 'O' then 'Open' else 'Closed' END as OPEN_CLOSE
	FROM `bcx-insights.telkom_customerexperience.orders_20191113_anon` orders
	left join (SELECT DISTINCT ACCOUNT_NO_ANON FROM `bcx-insights.telkom_customerexperience.disputes_20191113_anon`) disputes
	on orders.ACCOUNT_NO_ANON = disputes.ACCOUNT_NO_ANON)

	select * except(CUSTOMER_NO_ANON) from cte

	join  (select 
	CUSTOMER_BRAND,
	CUSTOMER_NO_ANON from `bcx-insights.telkom_customerexperience.customerdata_20191113_anon` 

	group by 
	CUSTOMER_NO_ANON,
	CUSTOMER_BRAND,
	CREDIT_CLASS_DESC,
	SOURCE) customers

	on customers.CUSTOMER_NO_ANON = cte.ACCOUNT_NO_ANON
	WHERE cte.ORDER_DATE BETWEEN '{0}' AND '{1}'
	GROUP BY cte.source,cte.ORIGINAL_SALES_CHANNEL_DESC ,cte.ACCOUNT_NO_ANON ,cte.has_dispute,cte.OPEN_CLOSE,customer_brand, cte.ORDER_DATE
	ORDER BY cte.source,cte.ORIGINAL_SALES_CHANNEL_DESC DESC
	LIMIT 100000
	""".format(start_date,end_date)

	Import_df = pd.read_gbq(Overall_Query,
	                    project_id = 'bcx-insights',
	                    dialect = 'standard')

	Data_df = Import_df.groupby('ORIGINAL_SALES_CHANNEL_DESC').filter(lambda x : len(x)>1000)
	Data_df_less = Import_df.groupby('ORIGINAL_SALES_CHANNEL_DESC').filter(lambda x : len(x)>1000)

	ID = ['Orders']      
	label = ['Orders']
	parent = ['']
	value = [len(Data_df)]
	ID2 = []
	label2 = []
	parent2 = []
	value2 = []
	for s in Data_df['source'].unique():
		ID.append('-'+s)
		label.append(s)
		parent.append('Orders')
		value.append(len(Data_df[Data_df['source']==s]))

		temp_df = Data_df[Data_df['source']==s]
		for osc in temp_df['ORIGINAL_SALES_CHANNEL_DESC'].unique():
			ID2.append(s+'-'+osc)
			label2.append(osc)
			parent2.append(s)
			value2.append(len(temp_df[temp_df['ORIGINAL_SALES_CHANNEL_DESC']==osc]))
	    
	ID_less = ['Other-Orders']      
	label_less = ['Orders']
	parent_less = ['']
	value_less = [len(Data_df_less)]
	ID2_less = []
	label2_less = []
	parent2_less = []
	value2_less = []
	for O in Data_df_less['OPEN_CLOSE'].unique():
		ID_less.append('-'+O)
		label_less.append(O)
		parent_less.append('Orders')
		value_less.append(len(Data_df_less[Data_df_less['OPEN_CLOSE']==O]))

		temp_df = Data_df_less[Data_df_less['OPEN_CLOSE']==O]
		for osc in temp_df['has_dispute'].unique():
			ID2_less.append(O+'-'+osc)
			label2_less.append(osc)
			parent2_less.append(O)
			value2_less.append(len(temp_df[temp_df['has_dispute']==osc]))

	Sun_ID = ID+ID2
	Sun_label = label+label2
	Sun_parent = parent+parent2
	Sun_value = value+value2

	Sun_ID_less = ID_less+ID2_less
	Sun_label_less = label_less+label2_less
	Sun_parent_less = parent_less+parent2_less
	Sun_value_less = value_less+value2_less


	sun =go.Figure(go.Sunburst(
	    labels=Sun_label,
	    parents=Sun_parent,
	    values=Sun_value,
	    branchvalues = 'total',
	    hoverinfo = 'all',
	    maxdepth=2
	))
	sun.update_layout(margin = dict(t=10, l=10, r=10, b=10))

	sunless =go.Figure(go.Sunburst(
	    labels=Sun_label_less,
	    parents=Sun_parent_less,
	    values=Sun_value_less,
	    branchvalues = 'total',
	    hoverinfo = 'all',
	    maxdepth=2
	))
	sunless.update_layout(margin = dict(t=10, l=10, r=10, b=10))
	return (sun, sunless)

	#return Sun_label, Sun_parent, Sun_value, Sun_label_less, Sun_parent_less, Sun_value_less

#Sun_label, Sun_parent, Sun_value, Sun_label_less, Sun_parent_less, Sun_value_less = generate_sunburst(start_date,end_date)

sun, sunless = generate_sunburst(start_date,end_date)

#sun =go.Figure(go.Sunburst(
    #labels=Sun_label,
    #parents=Sun_parent,
    #values=Sun_value,
    #branchvalues = 'total',
	#hoverinfo = 'all',
	##maxdepth=2
#))
#sun.update_layout(margin = dict(t=10, l=10, r=10, b=10))

#sunless= go.Figure(go.Sunburst(
	#labels=Sun_label_less,
	#parents=Sun_parent_less,
	#values=Sun_value_less,
	#branchvalues = 'total',
	#hoverinfo = 'all',
	##maxdepth=2
#))
#sunless.update_layout(margin = dict(t=10, l=10, r=10, b=10))

layout =  dcc.Loading(html.Div([
    html.H1('Telkom Customer Experience Home Page',style={'text-align':'center'}),
    html.Div([dcc.DatePickerRange(
    id = 'DatePicker',
    display_format='YYYY-MM-DD',
    start_date_placeholder_text='YYYY-MMM-DD',
    end_date_placeholder_text='YYYY-MMM-DD',
    max_date_allowed=dt.today().date(),
    end_date=dt.today().date(),
    start_date=(dt.today() - timedelta(days=90)).date(),
    style = {'width': '400px', 'display': 'inline-block'}),
    dbc.Button("Find Date",id='Run_Btn', color="primary", className="mr-1")]),
    html.Div([dcc.Graph(id = 'sunless1', figure=sunless, style={'width': '49%', 'display': 'flex', 'align-items': 'center', 'justify-content': 'center', 'float':'right'})]),
    html.Div([dcc.Graph(id = 'sun1',figure=sun, style={'width': '49%', 'display': 'flex', 'align-items': 'center', 'justify-content': 'center', 'float':'left'})]),
]))



@app.callback(
    [dash.dependencies.Output('sun1', 'figure'),
    dash.dependencies.Output('sunless1', 'figure')],
    [dash.dependencies.Input('Run_Btn', 'n_clicks')],
    [dash.dependencies.State('DatePicker', 'start_date'),
     dash.dependencies.State('DatePicker', 'end_date'),])
def update_output( n_clicks,start_date, end_date):
    if n_clicks is not None:
        return  generate_sunburst(start_date,end_date)
    else:
        raise PreventUpdate

if __name__ == '__main__':
    app.run_server(debug=False)
