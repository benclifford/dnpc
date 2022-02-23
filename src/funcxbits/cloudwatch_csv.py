
import csv
import datetime
import dateutil
import json

from uuid import UUID

from dnpc.structures import Context, Event

def import_cloudwatch(known_task_uuids, outer_context):

  luuids = set()
  with open('logs-insights-results.csv', newline='') as csvfile:
    reader = csv.reader(csvfile, ) # , delimiter=' ', quotechar='|')
    next(reader) #discard header row
    for row in reader:
        print("=== ROW ===")
        print(f"Time: {row[0]}")
        json_str = row[1]
        json_str = json_str.replace('\\r\n','')
        print(f"JSON: {json_str}")
 
        json_outer = json.loads(json_str)

        print(f"JSON deserialised: {json_outer}")

        json_inner_str = json_outer['log']

        json_inner = json.loads(json_inner_str)

        print(f"JSON inner: {json_inner}")
        assert isinstance(json_inner, dict)

        assert len(row) == 2

        task_uuid = UUID(json_inner['task_id'])

        if task_uuid in known_task_uuids:
            ctx = outer_context.get_context(task_uuid, "funcx.cloudwatch.task")  # TODO: are we allowed to use a UUID as a key in dnpc? probs should follow dict-like rules, so yes

            e = Event()
            e.time = dateutil.parser.isoparse(json_inner['asctime']).timestamp()
            
            e.type = json_inner['name'] + "-" + json_inner['message']

            ctx.events.append(e)  # TODO: do events have to be sorted in this structure? if so, then assert that in Context impl.
            ctx.events.sort(key=lambda e: e.time)  # i think no, but need to check

            luuids.add(task_uuid)

            if "times" in json_inner:
                times = json_inner['times']
                if times is not None:
                    execution_start = float(times['execution_start'])
                    execution_end = float(times['execution_end'])

                    times_ctx = ctx.get_context("times", "funcx.cloudwatch.task.times")

                    e = Event()
                    e.type = "execution_start"
                    e.time = execution_start
                    times_ctx.events.append(e)

                    e = Event()
                    e.type = "execution_end"
                    e.time = execution_end
                    times_ctx.events.append(e)


  return outer_context

  print(f"Found {len(luuids)} uuids matching")
