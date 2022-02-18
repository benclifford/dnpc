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
                  u = UUID(m.group(4).strip())
                except:
                  print(f"handling exception for uuid string >{m.group(4)}<")
                  raise
                known_task_uuids.add(u)
                uuidctx = root_context.get_context(u, "funcx.cloudwatch.task")
                ctx.alias_context("cloudwatch", uuidctx)

cloudwatch_ctx = import_cloudwatch(known_task_uuids, root_context)

print(root_context)


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


# Histograms of completion time differences
# there are various pairings available here.
# Without any log integration, only pair is
# between web service user fetched, and
# forwarder result received

def scan_context_for_result_to_fetch_duration(ctx):
  events = ctx.events
  enqueued = [e for e in events if e.type == "funcx_forwarder.forwarder-result_enqueued"]
  fetched = [e for e in events if e.type == "funcx_web_service-user_fetched"]
  if len(enqueued) != 1 or len(fetched) != 1:
    raise ValueError("Task does not have correct states for this plot")
    return []
  return [ fetched[0].time - enqueued[0].time ]

ctxs = cloudwatch_ctx.subcontexts_by_type("funcx.cloudwatch.task")
assert len(ctxs) == 500
ctx_durations = [scan_context_for_result_to_fetch_duration(c) for c in ctxs ]

durations= []
for d in ctx_durations:
  durations.extend(d)

xs = durations
print(xs)
assert len(xs) == 500


fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
plt.title("Result enqueued to user fetched duration")
hist, bins, _ = ax.hist(xs, bins=100)

plt.savefig("funcx-cloudwatch-enqueued-to-fetched-histo.png")


# histogram of user side task completion, and forward side result_enqueued
def scan_context_for_enqueued_to_client_completed_duration(ctx):
  subctx = ctx.subcontexts_by_type("funcx.cloudwatch.task")
  assert len(subctx) == 1
  events = subctx[0].events
  enqueued = [e for e in events if e.type == "funcx_forwarder.forwarder-result_enqueued"]

  events = ctx.events
  completed = [e for e in events if e.type == "POLL_END_COMPLETE"]

  if len(enqueued) != 1 or len(completed) != 1:
    raise ValueError("Task does not have correct states for this plot")
    return []
  return [ completed[0].time - enqueued[0].time ]

ctxs = root_context.subcontexts_by_type("demo.apptask")
assert len(ctxs) == 500
ctx_durations = [scan_context_for_enqueued_to_client_completed_duration(c) for c in ctxs ]

durations= []
for d in ctx_durations:
  durations.extend(d)

xs = durations
print(xs)
assert len(xs) == 500


fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
plt.title("Result enqueued to user fetched duration")
hist, bins, _ = ax.hist(xs, bins=100)

plt.savefig("funcx-cloudwatch-enqueued-to-client-completed-histo.png")


# histogram of user side task completion, and web service funcx_web_service-user_fetched"
def scan_context_for_user_fetched_to_client_completed_duration(ctx):
  subctx = ctx.subcontexts_by_type("funcx.cloudwatch.task")
  assert len(subctx) == 1
  events = subctx[0].events
  fetched = [e for e in events if e.type == "funcx_web_service-user_fetched"]

  events = ctx.events
  completed = [e for e in events if e.type == "POLL_END_COMPLETE"]

  if len(fetched) != 1 or len(completed) != 1:
    raise ValueError("Task does not have correct states for this plot")
    return []
  return [ completed[0].time - fetched[0].time ]

ctxs = root_context.subcontexts_by_type("demo.apptask")
assert len(ctxs) == 500
ctx_durations = [scan_context_for_user_fetched_to_client_completed_duration(c) for c in ctxs ]

durations= []
for d in ctx_durations:
  durations.extend(d)

xs = durations
print(xs)
assert len(xs) == 500


fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
plt.title("Web service user fetched to client side completed, duration")
hist, bins, _ = ax.hist(xs, bins=100)

plt.savefig("funcx-cloudwatch-user-fetched-to-client-completed-histo.png")




print("end import")
