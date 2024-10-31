import os

print(os.getcwd())

LOGS_PATH = "my_dagster_project/logs/log.txt"


with open(LOGS_PATH, 'r') as f:
    print(f.readlines())