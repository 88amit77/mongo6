from datetime import datetime
from dateutil.relativedelta import relativedelta

def lambda_handler():

    start_date1 = '2021-07-01'
    end_date1 = '2021-07-31'

    start_date1 = '2022-03-01'
    end_date1 = '2021-03-31'

    expecte_start_date='2022-02-01'
    expectend_date='20222-02-28'

    # emp_id = 172
    # emp_id = 151
    ####for curent month
    # start_date1 = '2021-05-01'
    print('start_date1', start_date1)
    start_date2 = "'" + start_date1 + "'"
    print('start_date2', start_date2)

    # end_date1 = '2021-05-31'
    print('end_date1', end_date1)
    end_date2 = "'" + end_date1 + "'"
    print('end_date1', end_date2)
    ####for curent month

    ###for previous month
    ###this you need to modify (harsh)
    start_date10 = '2021-06-01'
    print('start_date10', start_date10)
    start_date20 = "'" + start_date10 + "'"
    print('start_date20', start_date20)

    end_date10 = '2021-06-30'
    print('end_date10', end_date10)
    end_date20 = "'" + end_date10 + "'"
    print('end_date10', end_date20)
    print("done")


print(lambda_handler())
