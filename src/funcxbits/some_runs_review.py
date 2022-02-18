import re

import matplotlib.pyplot as plt

import numpy as np

from uuid import UUID

from dnpc.structures import Context, Event
from dnpc.plots import plot_context_streamgraph

from funcxbits.cloudwatch_csv import import_cloudwatch

# this is specifically aimed at importing the logs generated
# by funcxbits.do_some_runs

# import 'do_some_runs.log'

root_context = Context.new_root_context()

# this will be used to filter the cloudwatch logs to just the
# tasks imported by the first round of imports
known_task_uuids = set()

print("start import")
with open("do_some_runs.log", "r") as logfile:
    # 1645194062.952741 2022-02-18 14:21:02,952 dnpc.funcx.demoapp:27 MainProcess(157) MainThread [INFO]  TASK 0 RUN
    re_task = re.compile('([0-9.]*) .*  TASK ([^ ]*) ([^ \n]*)(.*)$')

    for line in logfile:
        m = re_task.match(line)
        if m:
            # print(f"Line matched TASK status: {line}, {m}")
            time = m.group(1)
            task_id = m.group(2)
            status = m.group(3)
            print(f"Time {time}, id {task_id}, status {status}")
            # task_id = m.group(1)
            # task_context = base_context.get_context(task_id, "parsl.task")
            # try_id = m.group(2)
            ctx = root_context.get_context(task_id, "demo.apptask")
            event = Event()
            event.type = status 
            event.time = float(time)
            ctx.events.append(event)

            # TASK 0 RUN_POST 8e7bfed8-56fa-450b-bdf3-93921820c8fb
            if status == "RUN_POST":  # extract the UUID to bind to funcx's task identity model
                try:
                  known_task_uuids.add(UUID(m.group(4).strip())) 
                except:
                  print(f"handling exception for uuid string >{m.group(4)}<")
                  raise

cloudwatch_ctx = import_cloudwatch(known_task_uuids)

print(root_context)

# can re-use a streamgraph plot from the LSST work to show the states of
# each task at each time?

ctxs = root_context.subcontexts_by_type("demo.apptask")

def filter_poll_start(ctx):
  new_ctx = Context()
  for e in ctx.events:
    if e.type != "POLL_START":
      new_ctx.events.append(e)
  return new_ctx

ctxs = [filter_poll_start(c) for c in ctxs]

colour_states={"RUN": "#FF0000",
               "RUN_POST": "#FF7777",
               "POLL_START": "#FFFF00",
               "POLL_END_COMPLETE": "#00FF00",
               "POLL_END_PENDING_running": "#FF00FF",
               "POLL_END_PENDING_waiting-for-launch": "#0000FF",
               "POLL_END_PENDING_waiting-for-nodes": "#00FFFF"
               }
plot_context_streamgraph(ctxs, "funcx-client-view.png", colour_states)

cloudwatch_colour_states = {"funcx_web_service-user_fetched": "#77FF22",
                            "funcx_forwarder.forwarder-result_enqueued": "#00EE00",
                            "funcx_web_service-received": "#FF7777",
                            "funcx_forwarder.forwarder-dispatched_to_endpoint": "#00FFFF"
                           }

plot_context_streamgraph(cloudwatch_ctx.subcontexts_by_type("funcx.cloudwatch.task"), "funcx-cloudwatch-view.png", cloudwatch_colour_states)

# TODO: histogram poll time (500 x many)

# need to go through each context in turn, scan its events and turn each POLL_START -> POLL_END_* status into a single value

def scan_context_for_poll_durations(ctx):
  events = ctx.events
  durations = []
  while events != []:
    e = events[0]
    events = events[1:] # remove paid of events
    if e.type == "POLL_START":
      e2 = events[0]
      events = events[1:] # remove paid of events
      if e2.type.startswith("POLL_END"):
        duration = e2.time - e.time
        durations.append(duration)
  return durations

ctxs = root_context.subcontexts_by_type("demo.apptask")
ctx_durations = [scan_context_for_poll_durations(c) for c in ctxs ]

durations= []
for d in ctx_durations:
  durations.extend(d)

xs = durations

fig = plt.figure()
ax = fig.add_subplot(2, 1, 1)
plt.title("Result poll duration")
hist, bins, _ = ax.hist(xs, bins=100)

ax = fig.add_subplot(2, 1, 2)
logbins = np.logspace(np.log10(bins[0]),np.log10(bins[-1]),len(bins))
hist, bins, _ = ax.hist(xs, bins=logbins)
plt.xscale('log')

plt.savefig("funcx-poll-duration-histo.png")

# TODO: histogram submission time (500)

print("end import")
