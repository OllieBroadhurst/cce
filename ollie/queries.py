
from datetime import datetime as dt

import pandas as pd

def build_query(iterable, field_name):
    if iterable is not None:
        if len(iterable) > 0:
            iterable = ','.join(["'"+s+"'" for s in iterable])
            iterable = f'and {field_name} IN ({iterable})'
            return iterable
        else:
            return ''
    else:
        return ''


def dispute_query(dispute_val, date_val):
    if dispute_val[0] == 'Yes':
        sql = f"""JOIN
        (SELECT DISTINCT ACCOUNT_NO_ANON dispute_id FROM
        `bcx-insights.telkom_customerexperience.disputes_20190903_00_anon`
        WHERE RESOLUTION_DATE > '{date_val}') disputes
        on orders.ACCOUNT_NO_ANON = disputes.dispute_id"""

        return sql, ''
    elif dispute_val[0] == 'No':
        sql = f"""LEFT JOIN
        (SELECT DISTINCT ACCOUNT_NO_ANON dispute_id FROM
        `bcx-insights.telkom_customerexperience.disputes_20190903_00_anon`
        WHERE RESOLUTION_DATE > '{date_val}') disputes
        on orders.ACCOUNT_NO_ANON = disputes.dispute_id"""

        return sql, "AND dispute_id is Null"
    else:
        return '', ''



def date_query(date_val):
    min_date_field = "MIN(orders.ORDER_CREATION_DATE)"
    min_date_criteria = f"""GROUP BY orders.ORDER_CREATION_DATE,
                        orders.ORDER_ID_ANON, orders.MSISDN_ANON,
                        orders.ACTION_TYPE_DESC
                        HAVING {min_date_field} >= '{date_val}'"""

    return f"{min_date_field},", min_date_criteria

def last_status_or_action_query(statuses, actions):

    status_field, status_where, status_group = '', '', ''
    action_field, action_where, action_group = '', '', ''

    if statuses is not None:
        if len(statuses) > 0:
            status_field = """LAST_VALUE(ACTION_STATUS_DESC) OVER
            (PARTITION BY ORDER_ID_ANON, MSISDN_ANON ORDER BY ORDER_ID_ANON, MSISDN_ANON, ORDER_CREATION_DATE
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_status_field,"""
            status_where = build_query(statuses, 'last_status_field')
            status_group = "ACTION_STATUS_DESC, "

    if actions is not None:
        if len(actions) > 0:
            action_field = """LAST_VALUE(ACTION_TYPE_DESC) OVER
            (PARTITION BY ORDER_ID_ANON, MSISDN_ANON ORDER BY ORDER_ID_ANON, MSISDN_ANON, ORDER_CREATION_DATE
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_action_type,"""

            action_group = "ACTION_TYPE_DESC, "
            action_where = build_query(actions, 'last_action_type')

    sql = f"""LEFT join
           (
           SELECT DISTINCT ORDER_ID_ANON, {action_field} {status_field} MSISDN_ANON
             FROM
            `bcx-insights.telkom_customerexperience.orders_20190926_00_anon`
            ) last_status

            on last_status.ORDER_ID_ANON = orders.ORDER_ID_ANON and
            last_status.MSISDN_ANON = orders.MSISDN_ANON"""

    return sql, status_where + ' ' + action_where



def criteria_tree_sql(service_type, customer_type, deal_desc, action_status,
                    date_val, dispute_val, action_filter):

    service_type = build_query(service_type, 'SERVICE_TYPE')
    customer_type = build_query(customer_type, 'CUSTOMER_TYPE_DESC')
    deal_desc = build_query(deal_desc, 'DEAL_DESC')
    dispute_join, dispute_where = dispute_query(dispute_val, date_val)

    action_status_subquery, action_status_where = last_status_or_action_query(action_status, action_filter)

    if date_val is not None:
        min_date_field, min_date_criteria = date_query(date_val)

    query = f"""WITH CTE as (
          SELECT DISTINCT orders.ORDER_CREATION_DATE,
          {min_date_field}
          orders.ORDER_ID_ANON,
          orders.MSISDN_ANON,
          ACTION_TYPE_DESC
           FROM `bcx-insights.telkom_customerexperience.orders_20190926_00_anon` orders

           LEFT JOIN

           (SELECT DISTINCT CUSTOMER_NO_ANON, SERVICE_TYPE, CUSTOMER_TYPE_DESC FROM
           `bcx-insights.telkom_customerexperience.customerdata_20190902_00_anon`) custs
           ON custs.CUSTOMER_NO_ANON = orders.ACCOUNT_NO_ANON

           {dispute_join}

           {action_status_subquery}

           WHERE 1 = 1
           {customer_type}
           {service_type}
           {deal_desc}
           {action_status_where}
           {dispute_where}

           {min_date_criteria}
          )

          SELECT *, ROW_NUMBER() OVER (PARTITION BY ORDER_ID_ANON, MSISDN_ANON ORDER BY ORDER_CREATION_DATE) Stage
          FROM CTE
          order by ORDER_ID_ANON, MSISDN_ANON, ORDER_CREATION_DATE DESC"""

    print(query)

    return query


if __name__ == '__main__':
    print(default_tree_sql())
