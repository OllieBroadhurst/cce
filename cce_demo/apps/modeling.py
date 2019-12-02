import os
import pickle
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, classification_report

import plotly.graph_objects as go


TRAIN_DATA_LIMIT = 2000000
PREDICT_DATA_LIMIT = 1800000


def data_preprocess(X):

    columns_to_drop = ['CUSTOMER_NO_ANON', 'BILL_MONTH']
    for c in columns_to_drop:
        if c in X.columns:
            X = X.drop(c, 1)

    if 'COMMITMENT_PERIOD' in X.columns:
        X['COMMITMENT_PERIOD'] = X['COMMITMENT_PERIOD'].astype(str)

    types = X.dtypes
    numeric_cols = list(types[types==float].index)
    categorical_cols = list(types[types!=float].index)

    nums = X[numeric_cols]
    cats = X[categorical_cols]

    cats = pd.get_dummies(cats, drop_first = False)

    X = cats.join(nums)

    if 'columns.txt' in os.listdir():
        columns = []
        with open('apps/columns.txt', 'r') as f:
            columns = [l.strip('\n') for l in f.readlines()]

        for c in X.columns:
            if c not in columns and c != 'Within_Dispute_Period':
                X = X.drop(c, axis=1)

        for c in columns:
            if c not in X.columns and c != 'Within_Dispute_Period':
                X[c] = 0

    X = X.sort_index(1)

    return X


def get_train_data(from_date_customer, from_date_dispute, row_limit):
    if row_limit > 0:
        row_limit = 'limit ' + str(row_limit)
    else:
        row_limit = ''

    query = """SELECT DISTINCT
            CUSTOMER_NO_ANON,
            COUNT(CUSTOMER_NO_ANON) OVER (PARTITION BY CUSTOMER_NO_ANON, OFFER_DESC) Months_With_Offer,
            CASE
                WHEN CUSTOMER_TYPE_DESC = 'Consumer' THEN 'Consumer'
                WHEN CUSTOMER_TYPE_DESC = 'Business' THEN 'Business'
            ELSE
                'Other'
            END as CUSTOMER_TYPE_DESC,
            COMMITMENT_PERIOD,
            TOTAL_AMOUNT,
            AVG(TOTAL_AMOUNT) OVER (Partition by CUSTOMER_NO_ANON, OFFER_DESC, PRIM_RESOURCE_VAL_ANON) Avg_Amount,
            STDDEV_POP(TOTAL_AMOUNT) OVER (Partition by CUSTOMER_NO_ANON, OFFER_DESC, PRIM_RESOURCE_VAL_ANON) Std_Amount,
            CASE
                WHEN LOWER(CREDIT_CLASS_DESC) = 'spclow' THEN 'special low'
                WHEN LOWER(CREDIT_CLASS_DESC) = 'highrisk' THEN 'high'
                WHEN LOWER(CREDIT_CLASS_DESC) = 'medrisk' THEN 'medium'
                WHEN LOWER(CREDIT_CLASS_DESC) = 'newrisk' THEN 'new'
                WHEN LOWER(CREDIT_CLASS_DESC) = 'lowrisk' THEN 'low'
            ELSE
                LOWER(CREDIT_CLASS_DESC)
            END as CREDIT_CLASS_DESC,
            SERVICE_TYPE,
            BILL_MONTH,
             IF(TIMESTAMP_DIFF(Disputes.CREATE_DATE, BILL_MONTH, DAY) < 32 AND TIMESTAMP_DIFF(Disputes.CREATE_DATE, BILL_MONTH, DAY) >=0, 1, 0) Within_Dispute_Period
            FROM `bcx-insights.telkom_customerexperience.customerdata_20191113_anon` Customers
            LEFT JOIN
            (SELECT DISTINCT ACCOUNT_NO_ANON, CREATE_DATE FROM `bcx-insights.telkom_customerexperience.disputes_20191113_anon`
            WHERE STATUS_OPEN_CLOSE = 'Justified' and CREATE_DATE >= '{0}') Disputes
            on Disputes.ACCOUNT_NO_ANON = Customers.CUSTOMER_NO_ANON
            WHERE CUSTOMER_TYPE_DESC <> 'Government' AND
            BILL_MONTH >= '{1}'
            {2}""".format(from_date_customer, from_date_dispute, row_limit)

    df = pd.io.gbq.read_gbq(query, project_id='bcx-insights', dialect='standard')

    return df


def get_current_customer_data(from_date_customer, to_date_customer, record_limit=''):
    if record_limit != '':
        record_limit = 'limit ' + str(record_limit)

    print('Fetching customer data. Please wait - this may take some time.')

    query = r"""
            with temp_table as (
            SELECT DISTINCT
            CUSTOMER_NO_ANON,
            OFFER_DESC,
            COUNT(CUSTOMER_NO_ANON) OVER (PARTITION BY CUSTOMER_NO_ANON, OFFER_DESC, PRIM_RESOURCE_VAL_ANON) Months_With_Offer,
            CASE
                WHEN CUSTOMER_TYPE_DESC in ('Consumer', 'Business') THEN CUSTOMER_TYPE_DESC
            ELSE
                'Other'
            END as CUSTOMER_TYPE_DESC,
            COMMITMENT_PERIOD,
            MAX(TOTAL_AMOUNT) OVER (Partition by CUSTOMER_NO_ANON, OFFER_DESC ORDER BY BILL_MONTH DESC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) -
            MIN(TOTAL_AMOUNT) OVER (Partition by CUSTOMER_NO_ANON, OFFER_DESC ORDER BY BILL_MONTH DESC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) Amount_Range,
            AVG(TOTAL_AMOUNT) OVER (Partition by CUSTOMER_NO_ANON, OFFER_DESC ORDER BY BILL_MONTH DESC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) Avg_Amount,
            STDDEV_POP(TOTAL_AMOUNT) OVER (Partition by CUSTOMER_NO_ANON, OFFER_DESC ORDER BY BILL_MONTH DESC ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) Std_Amount,
            CASE
                WHEN LOWER(CREDIT_CLASS_DESC) = 'spclow' THEN 'special low'
                WHEN LOWER(CREDIT_CLASS_DESC) = 'highrisk' THEN 'high'
                WHEN LOWER(CREDIT_CLASS_DESC) = 'medrisk' THEN 'medium'
                WHEN LOWER(CREDIT_CLASS_DESC) = 'newrisk' THEN 'new'
                WHEN LOWER(CREDIT_CLASS_DESC) = 'lowrisk' THEN 'low'
            ELSE
                LOWER(CREDIT_CLASS_DESC)
            END as CREDIT_CLASS_DESC,
            SERVICE_TYPE,
            BILL_MONTH
            FROM `bcx-insights.telkom_customerexperience.customerdata_20191113_anon`
            WHERE CUSTOMER_TYPE_DESC <> 'Government')
            SELECT * FROM TEMP_TABLE WHERE
            Months_With_Offer >= 4
            and BILL_MONTH BETWEEN '{0}' AND '{1}'
            ORDER BY Avg_Amount DESC
            {2}""".format(from_date_customer, to_date_customer, record_limit)

    df = pd.io.gbq.read_gbq(query, project_id='bcx-insights', dialect='standard')

    return df


def save_columns(from_date_customer, to_date_customer):
    query = r"""SELECT DISTINCT
    CASE
        WHEN CUSTOMER_TYPE_DESC in ('Consumer', 'Business') THEN CUSTOMER_TYPE_DESC
    ELSE
        'Other'
    END as CUSTOMER_TYPE_DESC,
    COMMITMENT_PERIOD,
    CASE
        WHEN LOWER(CREDIT_CLASS_DESC) = 'spclow' THEN 'special low'
        WHEN LOWER(CREDIT_CLASS_DESC) = 'highrisk' THEN 'high'
        WHEN LOWER(CREDIT_CLASS_DESC) = 'medrisk' THEN 'medium'
        WHEN LOWER(CREDIT_CLASS_DESC) = 'newrisk' THEN 'new'
        WHEN LOWER(CREDIT_CLASS_DESC) = 'lowrisk' THEN 'low'
    ELSE
        LOWER(CREDIT_CLASS_DESC)
    END as CREDIT_CLASS_DESC,
    IFNULL(SERVICE_TYPE, 'OTHER') SERVICE_TYPE
    FROM `bcx-insights.telkom_customerexperience.customerdata_20191113_anon` Customers
    WHERE CUSTOMER_TYPE_DESC <> 'Government' AND
    BILL_MONTH BETWEEN '{0}' AND '{1}'
    """.format(from_date_customer, to_date_customer)

    categorical_columns = pd.io.gbq.read_gbq(query, project_id='bcx-insights', dialect='standard')

    categorical_columns = data_preprocess(categorical_columns)
    categorical_columns = list(categorical_columns.columns)
    continuous_columns = ['Months_With_Offer', 'Had_Prior_Dispute', 'Avg_Amount', 'Std_Amount', 'Amount_Range']
    columns = categorical_columns + continuous_columns

    with open('apps/columns.txt', 'w') as f:
        f.writelines('\n'.join(columns))

    return '{} columns saved in columns.txt'.format(len(columns))

def upsample(data, repetitions):
    positive_samples = data[data['Within_Dispute_Period'] == 1]
    for _ in range(repetitions):
        data = data.append(positive_samples)
    return data.reset_index(drop=True)


def train_data(from_date_customer, from_date_dispute, data_limit=500000):
    df = get_train_data(from_date_customer, from_date_dispute, data_limit)

    #print(df['Within_Dispute_Period'].count())
    #dispute_prop = round(df['Within_Dispute_Period'].sum()/df['Within_Dispute_Period'].count(), 3)


    #df = upsample(df, int(0.2/dispute_prop))

    X = data_preprocess(df.drop('Within_Dispute_Period', 1))

    X = X.sort_index(1)
    y = df['Within_Dispute_Period']

    return X, y


def train_and_save_model():
    process_start_time = datetime.now()

    from_date_customer = (datetime.today().date() - timedelta(days=60)).strftime('%Y-%m-%d')
    from_date_dispute = (datetime.today().date() - timedelta(days=212)).strftime('%Y-%m-%d')

    X, y = train_data(from_date_customer, from_date_dispute, TRAIN_DATA_LIMIT)

    model = RandomForestClassifier(n_estimators=10, verbose=2)

    model.fit(X, y)

    print('Training complete. Time taken:', datetime.now() - process_start_time)

    with open('apps/model.pickle', 'wb') as model_file:
        pickle.dump(model, model_file)

    return model


def check_for_model(from_date, to_date):
    if 'model.pickle' not in os.listdir('apps/'):
        print('Model not found, training new model...')
        save_columns(from_date, to_date)
        model = train_and_save_model()
        print('Model trained')


def predict_probs(x):
    print('Model found')
    with open('apps/model.pickle', 'rb') as saved_model:
        model = pickle.load(saved_model)

    x = data_preprocess(x)
    x = x.sort_index(1)

    predictions = pd.DataFrame(model.predict_proba(x)[:, 1], columns=['probability'], index=x.index)
    return predictions


def default_risk_graph():
    default_axis_params = dict(showgrid=False,
                               zeroline=False,
                               showticklabels=False,
                               showline=False)

    return go.FigureWidget({'data':
        {
            'x': [],
            'y': [],
            'mode': 'markers',
            'marker': {'size': 1}
        },
        'layout': go.Layout(
            xaxis=default_axis_params,
            yaxis=default_axis_params
        )})
    #return go.Figure(data=[go.Bar(x=[], y=[])])


def get_bar_graph():
    start_time = datetime.now()
    from_date = datetime(2019, 9,1).strftime('%Y-%m-%d')
    to_date = datetime(2019, 11,13).strftime('%Y-%m-%d')
    #from_date = (datetime.today().date() - timedelta(days=61)).strftime('%Y-%m-%d')
    #to_date = datetime.today().date().strftime('%Y-%m-%d')

    check_for_model(from_date, to_date)
    x_pred = get_current_customer_data(from_date, to_date, PREDICT_DATA_LIMIT)

    summary_stats = x_pred[['OFFER_DESC', 'SERVICE_TYPE', 'Avg_Amount']]
    x_pred = x_pred.drop('OFFER_DESC', 1)

    accounts = x_pred['CUSTOMER_NO_ANON']

    graph_data = predict_probs(x_pred)
    x_pred = None

    graph_data = graph_data[graph_data['probability'] >= 0.09].drop_duplicates()
    graph_data['category'] = graph_data['probability'].apply(lambda x: 'green' if x < 0.5 else 'orange' if x < 0.75 else 'red')
    graph_data = graph_data.join(accounts)
    graph_data = graph_data.groupby('CUSTOMER_NO_ANON', as_index=False).max()

    model_table_data = graph_data[['CUSTOMER_NO_ANON', 'probability']]
    model_table_data = model_table_data.join(summary_stats)
    model_table_data = model_table_data.sort_values(['probability', 'Avg_Amount'], ascending=[False, False])

    model_table_data['Avg_Amount'] = model_table_data['Avg_Amount'].round(2)

    graph_data['probability'] = graph_data['probability'].round(5)
    graph_data['bin'] = pd.cut(graph_data['probability'], bins=5)

    graph_data['bin'] = graph_data['bin'].apply(lambda x: '{0} - {1}'.format(round(x.left,3), round(x.right, 3)))

    graph_data = graph_data.sort_values('bin').drop_duplicates()

    model_table_data = model_table_data.join(graph_data.set_index('CUSTOMER_NO_ANON')['bin'], on='CUSTOMER_NO_ANON')

    model_table_data.columns = ['Customer ID', 'Probability', 'Offer Description',
                                'Service Type', 'Average Amount', 'bin']

    graph_data = graph_data[['bin', 'CUSTOMER_NO_ANON', 'category']].groupby(['bin', 'category'], as_index=False).count()
    graph_data = graph_data.dropna()

    y = graph_data['CUSTOMER_NO_ANON']
    x = graph_data['bin'].astype(str)

    c = graph_data['category']

    fig = go.Figure(data=[go.Bar(x=x, y=y,
                marker_color=c)])

    print('Duration:', datetime.now() - start_time)

    return fig, model_table_data
