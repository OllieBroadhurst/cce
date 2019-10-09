import dash_core_components as dcc
import pandas as pd


def service_options():
    service_sql = """SELECT DISTINCT SERVICE_TYPE value FROM
    `bcx-insights.telkom_customerexperience.customerdata_20190902_00_anon`
    WHERE SERVICE_TYPE is not NULL AND CUSTOMER_NO_ANON in

    (SELECT DISTINCT ACCOUNT_NO_ANON FROM
    `bcx-insights.telkom_customerexperience.orders_20190903_00_anon`)"""

    options = pd.io.gbq.read_gbq(service_sql,
                                    project_id='bcx-insights',
                                    dialect='standard').fillna('N/A')

    options['label'] = options['value'].str.title()

    options = [{'label': x['label'],
                'value': x['value']} for _, x in options.iterrows()]

    return options


def customer_type():
    type_sql = """SELECT DISTINCT CUSTOMER_TYPE_DESC value FROM
    `bcx-insights.telkom_customerexperience.customerdata_20190902_00_anon`
    WHERE CUSTOMER_TYPE_DESC is not NULL AND CUSTOMER_NO_ANON in

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


def deal_desc():
    type_sql = r"""SELECT distinct DEAL_DESC,
    TRIM(REGEXP_REPLACE(DEAL_DESC, '(\\(|\\)|\\bR\\d*|\\d*(GB|MB|@|Mbps)|\\s\\d|\\+|\\b\\d\\b)', '')) DEAL
    FROM `bcx-insights.telkom_customerexperience.orders_20190926_00_anon`"""

    df = pd.io.gbq.read_gbq(type_sql,
                            project_id='bcx-insights',
                            dialect='standard').dropna().drop_duplicates()


    df['DEAL']  = df['DEAL'].str.split(' on')

    options = []
    for r in df['DEAL'] :
      options.append(r[-1].strip())

    df['DEAL'] = options
    deal_map = df.groupby('DEAL')['DEAL_DESC'].apply(list)
    deal_map = deal_map.to_dict()

    options = [{'label': k, 'value': str(v)} for k, v in deal_map.items()]

    return options


def action_status():
    type_sql = r"""SELECT distinct ACTION_STATUS_DESC
    FROM `bcx-insights.telkom_customerexperience.orders_20190926_00_anon`"""

    df = pd.io.gbq.read_gbq(type_sql,
                            project_id='bcx-insights',
                            dialect='standard').dropna().drop_duplicates()

    df = df['ACTION_STATUS_DESC'].tolist()

    options = [{'label': v, 'value': v} for v in df]

    return options

if __name__ == '__main__':
    print(service_options())
