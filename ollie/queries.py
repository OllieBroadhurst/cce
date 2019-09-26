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


def criteria_tree_sql(service_type):
    if service_type is not None:
        types = ','.join(["'"+s+"'" for s in service_type])

        return f"""WITH CTE as (
          SELECT DISTINCT ORDER_CREATION_DATE ORDER_CREATION_DATE,
          ORDER_ID_ANON,
          ACTION_TYPE_DESC
           FROM `bcx-insights.telkom_customerexperience.orders_20190903_00_anon` orders

           LEFT JOIN

           (SELECT DISTINCT CUSTOMER_NO_ANON, SERVICE_TYPE FROM
           `bcx-insights.telkom_customerexperience.customerdata_20190902_00_anon`) custs
           ON custs.CUSTOMER_NO_ANON = orders.ACCOUNT_NO_ANON

           WHERE ACCOUNT_NO_ANON in (5997992223372343676, 2637488041892950618)
           and SERVICE_TYPE IN ({types})

          )

          SELECT *, ROW_NUMBER() OVER (PARTITION BY ORDER_ID_ANON ORDER BY ORDER_CREATION_DATE) Stage
          FROM CTE
          order by ORDER_ID_ANON, ORDER_CREATION_DATE DESC"""

    else:
        return default_tree_sql()

if __name__ == '__main__':
    print(default_tree_sql())
