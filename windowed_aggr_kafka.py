from datetime import datetime, timedelta
from time import time
import random
import faust
import os
import json

class RawModel(faust.Record):
    date: datetime
    value: float


class AggModel(faust.Record):
    date: datetime
    count: int
    mean: float

TOPIC = 'raw_data'
SINK = 'aggregated_data'
TABLE = 'tumbling_table'
BROKER_STRING = os.environ['BROKER_STRING'].replace(',', ';')
KAFKA = 'kafka://' + BROKER_STRING
CLEANUP_INTERVAL = 1.0
WINDOW = 10
WINDOW_EXPIRES = 1
PARTITIONS = 1

app = faust.App('windowed-agg', broker=KAFKA, version=1, topic_partitions=PARTITIONS)

app.conf.table_cleanup_interval = CLEANUP_INTERVAL
source = app.topic(TOPIC, value_type=RawModel)
sink = app.topic(SINK, value_type=AggModel)

stream = app.stream(sink)

output_filename = "/home/ec2-user/output.json"

@app.task
async def get_aggregated():
    with open(output_filename, "a+") as f:
        async for value in stream:
            print(f"Outputting aggregated event to {output_filename}")
            out_object = { 'date': value.date, 'mean': value.mean }
            f.write(json.dumps(out_object) + "\n")
            f.flush()



def window_processor(key, events):
    timestamp = key[1][0]
    values = [event.value for event in events]
    count = len(values)
    mean = sum(values) / count

    print(
        f'processing window:'
        f'{len(values)} events,'
        f'mean: {mean:.2f},'
        f'timestamp {timestamp}',
    )

    sink.send_soon(value=AggModel(date=timestamp, count=count, mean=mean))

tumbling_table = (
    app.Table(
        TABLE,
        default=list,
        partitions=PARTITIONS,
        on_window_close=window_processor,
    )
    .tumbling(WINDOW, expires=timedelta(seconds=WINDOW_EXPIRES))
    .relative_to_field(RawModel.date)
)

@app.timer(0.1)
async def produce():
    await source.send(value=RawModel(value=random.random(), date=int(time())))


if __name__ == '__main__':
    app.main()

'''
what's happening in script so far:
The print_windowed_events function is processing events from the raw_data topic and updating the tumbling_table
The tumbling_table is calling the window_processor function when a window closes
The window_processor function is aggregating the events
The window_processor function is sending the aggregated data to the sink topic-description (your aggregated_data topic)

PTR:
You've defined a function called produce that will send a random value between zero and one to your raw_data topic
In a non-lab environment, this would be a source of real data, such as page-views, clicks, transactions, etc.
This function is generating a random value for the value field of your RawModel and a Unix timestamp for its date field
This function has a timer decorator and is using the async keyword
This means it will execute periodically and concurrently with other async functions
Also, note that you've configured the script to call the Faust application's main function when the script is run from a command-line.
'''

