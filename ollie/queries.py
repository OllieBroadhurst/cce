def default_tree_sql(id_list=[5997992223372343676, 2637488041892950618]):
    ids = ','.join([str(i) for i in id_list])

    return f"""WITH CTE as (
      SELECT DISTINCT ORDER_CREATION_DATE ORDER_CREATION_DATE,
      ORDER_ID_ANON,
      ACTION_TYPE_DESC
       FROM `bcx-insights.telkom_customerexperience.orders_20190903_00_anon`
       WHERE ACCOUNT_NO_ANON in ({ids})
      )

      SELECT *, ROW_NUMBER() OVER (PARTITION BY ORDER_ID_ANON ORDER BY ORDER_CREATION_DATE) Stage
      FROM CTE
      order by ORDER_ID_ANON, ORDER_CREATION_DATE DESC"""


def criteria_tree_sql(service_type, customer_type):
    if service_type is not None:
        service_types = ','.join(["'"+s+"'" for s in service_type])
        service_types = f'and SERVICE_TYPE IN ({service_types})'
    else:
        service_types = ''

    if customer_type is not None:
        customer_types = ','.join(["'"+c+"'" for c in customer_type])
        customer_types = f'and CUSTOMER_TYPE_DESC IN ({customer_types})'
    else:
        customer_types = ''


    return f"""WITH CTE as (
          SELECT DISTINCT ORDER_CREATION_DATE ORDER_CREATION_DATE,
          ORDER_ID_ANON,
          ACTION_TYPE_DESC
           FROM `bcx-insights.telkom_customerexperience.orders_20190903_00_anon` orders

           LEFT JOIN

           (SELECT DISTINCT CUSTOMER_NO_ANON, SERVICE_TYPE, CUSTOMER_TYPE_DESC FROM
           `bcx-insights.telkom_customerexperience.customerdata_20190902_00_anon`) custs
           ON custs.CUSTOMER_NO_ANON = orders.ACCOUNT_NO_ANON

           WHERE ACCOUNT_NO_ANON in (5997992223372343676, 2637488041892950618)
           {customer_types}
           {service_types}
          )

          SELECT *, ROW_NUMBER() OVER (PARTITION BY ORDER_ID_ANON ORDER BY ORDER_CREATION_DATE) Stage
          FROM CTE
          order by ORDER_ID_ANON, ORDER_CREATION_DATE DESC"""


if __name__ == '__main__':
    print(default_tree_sql())
