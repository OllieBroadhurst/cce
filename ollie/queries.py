
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
    sql = f"""
        (SELECT DISTINCT ACCOUNT_NO_ANON dispute_id FROM
        `bcx-insights.telkom_customerexperience.disputes_20190903_00_anon`
        WHERE RESOLUTION_DATE > '{date_val}') disputes
        on orders.ACCOUNT_NO_ANON = disputes.dispute_id"""

    if dispute_val == 'Yes':
        join_type = 'JOIN '
        return join_type + sql, ''
    elif dispute_val == 'No':
        join_type = 'LEFT JOIN '

        return join_type + sql, "AND dispute_id is Null"
    else:
        return '', ''


def fault_query(fault_val, date_val):
    sql = f"""
        (SELECT DISTINCT SERVICE_KEY_ANON fault_id FROM
        `bcx-insights.telkom_customerexperience.faults_20190903_00_anon`
        WHERE DATDRGT > '{date_val}') faults
        on orders.ACCOUNT_NO_ANON = faults.fault_id"""

    if fault_val == 'Yes':
        join_type = 'JOIN '
        return join_type + sql, ''
    elif fault_val == 'No':
        join_type = 'LEFT JOIN '

        return join_type + sql, "AND fault_id is Null"
    else:
        return '', ''


def date_query(date_val):
    min_date_field = "MIN(orders.ORDER_CREATION_DATE)"
    min_date_criteria = f"""GROUP BY orders.ORDER_CREATION_DATE,
                        orders.ORDER_ID_ANON, orders.MSISDN_ANON,
                        orders.ACTION_TYPE_DESC, ACCOUNT_NO_ANON
                        HAVING {min_date_field} >= '{date_val}'"""

    return f"{min_date_field},", min_date_criteria


def last_status_or_action_query(statuses, actions):

    status_field, status_where, status_group = '', '', ''
    action_field, action_where, action_group = '', '', ''

    if statuses is not None:
        if len(statuses) > 0:
            status_field = """LAST_VALUE(ACTION_STATUS_DESC) OVER
            (PARTITION BY ORDER_ID_ANON, MSISDN_ANON ORDER BY ORDER_ID_ANON, MSISDN_ANON, ORDER_CREATION_DATE, ACTION_TYPE_DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_status_field,"""
            status_where = build_query(statuses, 'last_status_field')
            status_group = "ACTION_STATUS_DESC, "

    if actions is not None:
        if len(actions) > 0:
            actions = [a.lower() for a in actions]

            action_field = """LAST_VALUE(ACTION_TYPE_DESC) OVER
            (PARTITION BY ORDER_ID_ANON, MSISDN_ANON ORDER BY ORDER_ID_ANON, MSISDN_ANON, ORDER_CREATION_DATE, ACTION_TYPE_DESC
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_action_type,"""

            action_group = "ACTION_TYPE_DESC, "
            action_where = build_query(actions, 'last_action_type')
            action_where = action_where.replace('last_action_type', "lower(last_action_type)")

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
                    date_val, dispute_val, action_filter, fault_val):

    service_type = build_query(service_type, 'SERVICE_TYPE')
    customer_type = build_query(customer_type, 'CUSTOMER_TYPE_DESC')
    deal_desc = build_query(deal_desc, 'DEAL_DESC')
    dispute_join, dispute_where = dispute_query(dispute_val, date_val)
    fault_join, fault_where = fault_query(fault_val, date_val)

    action_status_subquery, action_status_where = last_status_or_action_query(action_status, action_filter)

    if date_val is not None:
        min_date_field, min_date_criteria = date_query(date_val)

    query = f"""WITH CTE as (
          SELECT DISTINCT
          orders.ACCOUNT_NO_ANON,
          orders.ORDER_CREATION_DATE,
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
           {fault_join}

           {action_status_subquery}

           WHERE 1 = 1
           {customer_type}
           {service_type}
           {deal_desc}
           {action_status_where}
           {dispute_where}
           {fault_where}

           {min_date_criteria}
          )

          SELECT *, ROW_NUMBER() OVER (PARTITION BY ORDER_ID_ANON, MSISDN_ANON ORDER BY ORDER_CREATION_DATE, ACTION_TYPE_DESC) Stage
          FROM CTE
          order by ORDER_ID_ANON, MSISDN_ANON, ORDER_CREATION_DATE DESC"""

    return query


if __name__ == '__main__':
    print(default_tree_sql())
