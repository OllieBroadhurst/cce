import pandas as pd

from tensorflow.keras.models import Sequential, load_model
from tensorflow.keras.layers import Dense, Dropout
from tensorflow.keras.callbacks import EarlyStopping

from sklearn.metrics import confusion_matrix, classification_report
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler

import os

import plotly.graph_objects as go

def get_data(row_limit, from_date_customer, from_date_dispute):
    if row_limit > 0:
        row_limit = 'limit ' + str(row_limit)
    else:
        row_limit = ''

    query = """SELECT DISTINCT
            ACCOUNT_NO_ANON,
            COUNT(ACCOUNT_NO_ANON) OVER (PARTITION BY ACCOUNT_NO_ANON, OFFER_DESC) Months_With_Offer,
            CASE
                WHEN CUSTOMER_TYPE_DESC = 'Consumer' THEN 'Consumer'
                WHEN CUSTOMER_TYPE_DESC = 'Business' THEN 'Business'
            ELSE
                'Other'
            END as CUSTOMER_TYPE_DESC,
            OFFER_DESC,
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
            Disputes.RESOLUTION_DATE,
             IF(TIMESTAMP_DIFF(Disputes.RESOLUTION_DATE, BILL_MONTH, DAY) < 122 AND TIMESTAMP_DIFF(Disputes.RESOLUTION_DATE, BILL_MONTH, DAY) >=0, 1, 0) Within_Dispute_Period
            FROM `bcx-insights.telkom_customerexperience.customerdata_20190902_00_anon` Customers
            LEFT JOIN
            (SELECT DISTINCT ACCOUNT_NO_ANON, RESOLUTION_DATE FROM `bcx-insights.telkom_customerexperience.disputes_20190903_00_anon`
            WHERE STATUS_DESC = 'Justified' and RESOLUTION_DATE >= '{0}') Disputes
            on Disputes.ACCOUNT_NO_ANON = Customers.CUSTOMER_NO_ANON
            WHERE BILL_MONTH >= '{1}'
            {2}""".format(from_date_customer, from_date_dispute, row_limit)

    df = pd.io.gbq.read_gbq(query, project_id='bcx-insights', dialect='standard')

    return df


def preprocess(df):
    columns_to_drop = ['ACCOUNT_NO_ANON', 'RESOLUTION_DATE', 'BILL_MONTH']
    df = df.drop(columns_to_drop, 1)
    df['COMMITMENT_PERIOD'] = df['COMMITMENT_PERIOD'].astype(str)

    types = df.dtypes

    numeric_cols = list(types[types==float].index)
    categorical_cols = list(types[types!=float].index)

    ss = StandardScaler()
    scale_cols = pd.DataFrame(ss.fit_transform(df[numeric_cols]), columns=numeric_cols, index=df.index)

    df = df[categorical_cols].join(scale_cols)
    df = pd.get_dummies(df)

    return df


def upsample(data, repetitions):
    positive_samples = data[data['Within_Dispute_Period'] == 1]
    for _ in range(repetitions):
        data = data.append(positive_samples)
    return data.reset_index(drop=True)


def train_test_data(data_limit=500000):
    df = get_data(data_limit)

    dispute_prop = round(df['Within_Dispute_Period'].sum()/df['Within_Dispute_Period'].count(), 3)

    df = upsample(df, int(0.2/dispute_prop))
    X = preprocess(df.drop('Within_Dispute_Period', 1))
    y = df['Within_Dispute_Period']

    X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=42, test_size=0.3)

    return X_train, X_test, y_train, y_test


def train_and_save_model():
    data_limit = 500000
    X_train, X_test, y_train, y_test = train_test_data(data_limit)

    model = Sequential()

    model.add(Dense(16, input_dim=X.shape[1], activation='relu'))
    model.add(Dense(8, activation='relu'))
    model.add(Dense(1, activation='sigmoid'))

    es = EarlyStopping(monitor='val_loss', patience=3)

    model.compile(optimizer='adam',
                  loss='binary_crossentropy',
                  metrics=['accuracy']);

    model.fit(X_train, y_train,
            validation_data=[X_test, y_test],
            epochs=20, callbacks=[es],
           batch_size=data_limit//5)

    model.save('dispute_model.h5')


def predict_probs(x):
    if 'model.h5' not in os.listdir():
        train_and_save_model()

    model = load_model('model.h5')

    accounts = x['ACCOUNT_NO_ANON']
    x = preprocess(x)

    predictions = pd.DataFrame(model.predict(x), columns=['probability'], index=x.index)
    return predictions


def get_current_customer_data():
    return get_data(100000, '2019-07-01', '2019-04-01')


def default_risk_graph():
    return go.Figure(data=[go.Bar(x=[], y=[])])


def get_bar_graph():
    x_pred = get_current_customer_data()
    graph_data = predict_probs(x_pred)

    graph_data = graph_data[graph_data['probability'] >= 0.3]
    graph_data['category'] = graph_data['probability'].apply(lambda x: 'green' if x <= 0.5 else 'orange' if x < 0.75 else 'red')
    graph_data = graph_data.join(accounts)
    graph_data = graph_data.groupby('CUSTOMER_NO_ANON', as_index=False).max()
    graph_data['bin'] = pd.cut(graph_data['probability'], bins=15).apply(lambda x: str(x.right) if x.left > graph_data['probability'].min() else '{0} - {1}'.format(x.left, x.right))
    graph_data = graph_data.sort_values('bin')

    graph_data = graph_data[['bin', 'CUSTOMER_NO_ANON', 'category']].groupby(['bin', 'category'], as_index=False).count()

    y = graph_data['CUSTOMER_NO_ANON']
    x = graph_data['bin']
    c = graph_data['category']

    fig = go.Figure(
                data=[go.Bar(x=x, y=y)],
                marker=dict(color=c))

    return fig
