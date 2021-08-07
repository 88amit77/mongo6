# import croniter
# import datetime
#
# now = datetime.datetime.now()
# # sched = '1 15 1,15 * *'    # at 3:01pm on the 1st and 15th of every month
# sched = '1 15 1,15 * *'    # at 3:01pm on the 1st and 15th of every month
# cron = croniter.croniter(sched, now)
#
# for i in range(2):
#     nextdate = cron.get_next(datetime.datetime)
#     print(nextdate)
#
import json
import croniter
import datetime


def lambda_handler():
    now = datetime.datetime.now()
    # sched = '1 15 1,15 * *'    # at 3:01pm on the 1st and 15th of every month
    sched = '1 15 1,15 * *'  # at 3:01pm on the 1st and 15th of every month
    cron = croniter.croniter(sched, now)

    for i in range(2):
        nextdate = cron.get_next(datetime.datetime)
        print(nextdate)
    nextdate1 = str(nextdate)
    return {
        'statusCode': 200,
        'next_due_date': nextdate1
    }

print(lambda_handler())