def build_query(iterable, field_name):
    if iterable is not None:
        if len(iterable) > 0:
            iterable = ','.join(["'" + s + "'" for s in iterable])
            iterable = f'and {field_name} IN ({iterable})'
            return iterable
        else:
            return ''
    else:
        return ''


def dispute_query(dispute_val, start_date_val, end_date_val):
    sql = f"""
        (SELECT DISTINCT ACCOUNT_NO_ANON dispute_id FROM
        `bcx-insights.telkom_customerexperience.disputes_20190903_00_anon`
        WHERE RESOLUTION_DATE BETWEEN '{start_date_val}' AND '{end_date_val}') disputes
        on orders.ACCOUNT_NO_ANON = disputes.dispute_id"""

    if dispute_val == 'Yes':
        join_type = 'JOIN '
        return join_type + sql, ''
    elif dispute_val == 'No':
        join_type = 'LEFT JOIN '

        return join_type + sql, "AND dispute_id is Null"
    else:
        return '', ''


def fault_query(fault_val, start_date_val, end_date_val):
    sql = f"""
        (SELECT DISTINCT SERVICE_KEY_ANON fault_id FROM
        `bcx-insights.telkom_customerexperience.faults_20190903_00_anon`
        WHERE DATDRGT BETWEEN '{start_date_val}' AND '{end_date_val}') faults
        on orders.ACCOUNT_NO_ANON = faults.fault_id"""

    if fault_val == 'Yes':
        join_type = 'JOIN '
        return join_type + sql, ''
    elif fault_val == 'No':
        join_type = 'LEFT JOIN '

        return join_type + sql, "AND fault_id is Null"
    else:
        return '', ''


def date_query(start_date_val, end_date_val):
    min_date_field = "MIN(orders.ORDER_CREATION_DATE)"
    min_date_criteria = f"""GROUP BY orders.ORDER_CREATION_DATE,
                        orders.ORDER_ID_ANON, orders.MSISDN_ANON,
                        orders.ACTION_TYPE_DESC, ACCOUNT_NO_ANON, ACTION_STATUS_DESC
                        HAVING {min_date_field} BETWEEN '{start_date_val}' AND '{end_date_val}'"""

    return f"{min_date_field},", min_date_criteria


def includes_action(action_list, start_date_val, end_date_val):
    if action_list is not None:
        if len(action_list) > 0:
            action_list = ','.join(["'" + a + "'" for a in action_list])
            sql = f""" AND concat(cast(orders.ORDER_ID_ANON as string), cast(orders.MSISDN_ANON as string))
            in (SELECT DISTINCT concat(cast(ORDER_ID_ANON as string), cast(MSISDN_ANON as string)) FROM
            `bcx-insights.telkom_customerexperience.orders_20190926_00_anon`
            WHERE ACTION_TYPE_DESC IN ({action_list})
            AND ORDER_CREATION_DATE BETWEEN '{start_date_val}' AND '{end_date_val}')"""
            return sql
    return ''


def last_status_or_action_query(statuses, actions):
    status_field, status_where, status_group = '', '', ''
    action_field, action_where, action_group = '', '', ''

    if statuses is not None:
        if len(statuses) > 0:
            status_field = """LAST_VALUE(ACTION_STATUS_DESC) OVER
            (PARTITION BY orders.ACCOUNT_NO_ANON, orders.ORDER_ID_ANON ORDER BY ACCOUNT_NO_ANON, ORDER_ID_ANON, ORDER_CREATION_DATE
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_status_field,"""
            status_where = build_query(statuses, 'last_status_field')
            status_group = "ACTION_STATUS_DESC, "

    if actions is not None:
        if len(actions) > 0:
            actions = [a.lower() for a in actions]

            action_field = """LAST_VALUE(ACTION_TYPE_DESC) OVER
            (PARTITION BY orders.ACCOUNT_NO_ANON, orders.ORDER_ID_ANON ORDER BY ACCOUNT_NO_ANON, ORDER_ID_ANON, ORDER_CREATION_DATE
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) last_action_type,"""

            action_group = "ACTION_TYPE_DESC, "
            action_where = build_query(actions, 'last_action_type')
            action_where = action_where.replace('last_action_type', "lower(last_action_type)")

    if status_field != '' and action_field != '':
        sql = f"""LEFT join
               (
               SELECT DISTINCT ORDER_ID_ANON, {action_field} {status_field} MSISDN_ANON
                 FROM
                `bcx-insights.telkom_customerexperience.orders_20190926_00_anon`
                ) last_status

                on last_status.ORDER_ID_ANON = orders.ORDER_ID_ANON and
                last_status.MSISDN_ANON = orders.MSISDN_ANON"""

        return sql, status_where + ' ' + action_where
    else:
        return '', ''


def build_min_hours(min_hours):
    if min_hours > 0:
        sql = """TIMESTAMP_DIFF(MAX(ORDER_CREATION_DATE) OVER (Partition by orders.ACCOUNT_NO_ANON, orders.ORDER_ID_ANON),
                MIN(ORDER_CREATION_DATE) OVER (Partition by orders.ACCOUNT_NO_ANON, orders.ORDER_ID_ANON), HOUR) j_duration,"""

        min_hours_where = f'AND j_duration >= {min_hours}'

        return sql, min_hours_where
    return '', ''


def criteria_tree_sql(service_type, customer_type, deal_desc, action_status,
                      start_date_val, end_date_val, dispute_val, action_filter,
                      fault_val, min_hours, has_action):
    service_type = build_query(service_type, 'SERVICE_TYPE')
    customer_type = build_query(customer_type, 'CUSTOMER_TYPE_DESC')
    deal_desc = build_query(deal_desc, 'DEAL_DESC')
    has_action = includes_action(has_action, start_date_val, end_date_val)

    dispute_join, dispute_where = dispute_query(dispute_val, start_date_val, end_date_val)
    fault_join, fault_where = fault_query(fault_val, start_date_val, end_date_val)
    hours_sql_field, hours_where = build_min_hours(min_hours)

    action_status_subquery, action_status_where = last_status_or_action_query(action_status, action_filter)

    if start_date_val is not None and end_date_val is not None:
        min_date_field, min_date_criteria = date_query(start_date_val, end_date_val)
    else:
        min_date_field = ''
        min_date_criteria = ''

    query = f"""WITH CTE as (
          SELECT DISTINCT
          orders.ACCOUNT_NO_ANON,
          orders.ORDER_CREATION_DATE,
          MIN(orders.ORDER_CREATION_DATE),
          {min_date_field}
          {hours_sql_field}
          orders.ORDER_ID_ANON,
          orders.MSISDN_ANON,
          CONCAT(ACTION_TYPE_DESC, ' (', ACTION_STATUS_DESC, ')') ACTION_TYPE_DESC
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
           {has_action}
           {action_status_where}
           {dispute_where}
           {fault_where}

           {min_date_criteria}
          ),

          STAGES_ADDED AS (

          SELECT ACCOUNT_NO_ANON, ORDER_CREATION_DATE, ORDER_ID_ANON,
          MSISDN_ANON, ACTION_TYPE_DESC,
          DENSE_RANK() OVER (PARTITION BY ACCOUNT_NO_ANON, ORDER_ID_ANON ORDER BY

          CAST (
            CONCAT(
              CAST(EXTRACT(YEAR FROM ORDER_CREATION_DATE) as STRING),
              CAST(EXTRACT(MONTH FROM ORDER_CREATION_DATE) as STRING),
              CAST(EXTRACT(DAY FROM ORDER_CREATION_DATE) as STRING),
              CAST(EXTRACT(MINUTE FROM ORDER_CREATION_DATE) as STRING)
                  )
          as INT64)

          ) Stage

          FROM CTE WHERE 1 = 1
          {hours_where})

          SELECT DISTINCT
            ACCOUNT_NO_ANON,
            ORDER_CREATION_DATE,
            STAGES_ADDED.ORDER_ID_ANON,
            STAGES_ADDED.MSISDN_ANON,
            ACTION_TYPE_DESC,
            STAGES_ADDED.Stage,
            STAGES_ADDED.Stage + 1 Next_Stage,
            IFNULL(Durations.Duration, 0) Duration

          FROM STAGES_ADDED

          LEFT JOIN

          (SELECT * FROM
            (SELECT Stage, ORDER_ID_ANON,
            timestamp_diff(ORDER_CREATION_DATE, LAG(ORDER_CREATION_DATE) OVER (PARTITION BY ACCOUNT_NO_ANON, ORDER_ID_ANON ORDER BY Stage), HOUR) Duration
            FROM STAGES_ADDED)
          WHERE Duration is not Null and Duration > 0
          ) Durations

          on Durations.Stage = STAGES_ADDED.Stage
          and Durations.ORDER_ID_ANON = STAGES_ADDED.ORDER_ID_ANON
          order by STAGES_ADDED.ORDER_ID_ANON, STAGES_ADDED.Stage, STAGES_ADDED.MSISDN_ANON"""
    
    return query