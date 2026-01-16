from huey import RedisHuey, crontab


huey1 = RedisHuey('app1', host='localhost')
huey2 = RedisHuey('app2', host='localhost')


@huey1.periodic_task(crontab(minute="*"))
def periodic_task_1():
    print(periodic_task_1)


@huey2.periodic_task(crontab(minute="*"))
def periodic_task_2():
    print(periodic_task_2)
