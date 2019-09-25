import dash_core_components as dcc
import pandas as pd


def service_options():
    service_sql = """SELECT DISTINCT SERVICE_TYPE value FROM
    `bcx-insights.telkom_customerexperience.customerdata_20190902_00_anon`"""

    options = pd.io.gbq.read_gbq(service_sql,
                                    project_id='bcx-insights',
                                    dialect='standard').fillna('N/A')

    options['label'] = options['value'].str.title()

    options = [{'label': x['label'],
                'value': x['value']} for _, x in options.iterrows()]

    return options


if __name__ == '__main__':
    print(service_options())
