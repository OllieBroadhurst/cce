
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


def criteria_tree_sql(service_type, customer_type, deal_desc):

    service_type = build_query(service_type, 'SERVICE_TYPE')
    customer_type = build_query(customer_type, 'CUSTOMER_TYPE_DESC')
    deal_desc = build_query(deal_desc, 'DEAL_DESC')


    return f"""WITH CTE as (
          SELECT DISTINCT ORDER_CREATION_DATE ORDER_CREATION_DATE,
          ORDER_ID_ANON,
          MSISDN_ANON,
          ACTION_TYPE_DESC
           FROM `bcx-insights.telkom_customerexperience.orders_20190926_00_anon` orders

           LEFT JOIN

           (SELECT DISTINCT CUSTOMER_NO_ANON, SERVICE_TYPE, CUSTOMER_TYPE_DESC FROM
           `bcx-insights.telkom_customerexperience.customerdata_20190902_00_anon`) custs
           ON custs.CUSTOMER_NO_ANON = orders.ACCOUNT_NO_ANON

           WHERE 1 = 1
           {customer_type}
           {service_type}
           {deal_desc}
          )

          SELECT *, ROW_NUMBER() OVER (PARTITION BY ORDER_ID_ANON, MSISDN_ANON ORDER BY ORDER_CREATION_DATE) Stage
          FROM CTE
          order by ORDER_ID_ANON, MSISDN_ANON, ORDER_CREATION_DATE DESC"""


if __name__ == '__main__':
    print(default_tree_sql())
