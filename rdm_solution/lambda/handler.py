import os

def handler(event, context):
    env = os.environ['ENV']
    return f'{env}'