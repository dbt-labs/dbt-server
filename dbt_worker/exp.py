from dbt_worker.tasks_temp import add

task_obj = (add.delay(2, 2))
print(task_obj.id)

# .revoke(signal = "SIGKILL")
