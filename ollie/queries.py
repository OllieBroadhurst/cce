
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


def status_query(statuses):
    statuses = ','.join(["'"+s+"'" for s in statuses])

    sql = f"""LEFT join
           (
            SELECT DISTINCT ORDER_ID_ANON, ACTION_STATUS_DESC last_status_field, ORDER_CREATION_DATE, MAX(ORDER_CREATION_DATE) FROM
            `bcx-insights.telkom_customerexperience.orders_20190926_00_anon`
            group by ORDER_ID_ANON, ACTION_STATUS_DESC, ORDER_CREATION_DATE
            HAVING ORDER_CREATION_DATE = MAX(ORDER_CREATION_DATE)
            ) last_status

            on last_status.ORDER_ID_ANON = orders.ORDER_ID_ANON and
            last_status.ORDER_CREATION_DATE = orders.ORDER_CREATION_DATE"""


    return sql, 'last_status_field'

def criteria_tree_sql(service_type, customer_type, deal_desc, action_status):

    service_type = build_query(service_type, 'SERVICE_TYPE')
    customer_type = build_query(customer_type, 'CUSTOMER_TYPE_DESC')
    deal_desc = build_query(deal_desc, 'DEAL_DESC')

    if action_status is None:
        action_status = ''
        status_subquery = ''
    elif len(action_status) > 0:
        status_subquery, last_status_field = status_query(action_status)
        action_status = build_query(action_status, last_status_field)
    else:
        action_status = ''
        status_subquery = ''

    return f"""WITH CTE as (
          SELECT DISTINCT orders.ORDER_CREATION_DATE,
          orders.ORDER_ID_ANON,
          MSISDN_ANON,
          ACTION_TYPE_DESC
           FROM `bcx-insights.telkom_customerexperience.orders_20190926_00_anon` orders

           LEFT JOIN

           (SELECT DISTINCT CUSTOMER_NO_ANON, SERVICE_TYPE, CUSTOMER_TYPE_DESC FROM
           `bcx-insights.telkom_customerexperience.customerdata_20190902_00_anon`) custs
           ON custs.CUSTOMER_NO_ANON = orders.ACCOUNT_NO_ANON

           {status_subquery}

           WHERE 1 = 1
           {customer_type}
           {service_type}
           {deal_desc}
           {action_status}
          )

          SELECT *, ROW_NUMBER() OVER (PARTITION BY ORDER_ID_ANON, MSISDN_ANON ORDER BY ORDER_CREATION_DATE) Stage
          FROM CTE
          order by ORDER_ID_ANON, MSISDN_ANON, ORDER_CREATION_DATE DESC"""


if __name__ == '__main__':
    print(default_tree_sql())
