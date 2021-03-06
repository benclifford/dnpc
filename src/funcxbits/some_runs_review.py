import re

import pandas 

import matplotlib.pyplot as plt

import numpy as np

import random

from uuid import UUID

from dnpc.structures import Context, Event
from dnpc.plots import plot_context_streamgraph

from funcxbits.cloudwatch_csv import import_cloudwatch, import_file

# There are three log sources at the moment, and different sets
# of results can be calculated depending on which is available:

source_clientside = True

# the central hosted services data - either cloudwatch or from k8s copied file
#source_central = "file"
source_central = False

# endpoint logs - I'm not doing anything with these
source_endpoint = False

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

    # 1645568761.984889 2022-02-22 22:26:01,984 dnpc.funcx.demoapp:60 MainProcess(1082) MainThread [INFO]  TASK_INNER_TIME 497 1645568751.3486533 1645568761.355121

    re_result = re.compile('[0-9.]* .*  TASK_INNER_TIME ([0-9]+) ([0-9.]+) ([0-9.]+).*$')


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
            print(f"XX1: import TASK status, task id >{task_id}<")
            ctx = root_context.get_context(task_id, "demo.apptask")
            event = Event()
            event.type = status 
            event.time = float(time)
            ctx.events.append(event)

            # TASK 0 SUBMIT_POST 8e7bfed8-56fa-450b-bdf3-93921820c8fb
            if status == "SUBMIT_POST":  # extract the UUID to bind to funcx's task identity model
                try:
                  u = UUID(m.group(4).strip())
                except:
                  print(f"handling exception for uuid string >{m.group(4)}<")
                  raise
                known_task_uuids.add(u)
                uuidctx = root_context.get_context(u, "funcx.cloudwatch.task")
                ctx.alias_context("cloudwatch", uuidctx)

        m = re_result.match(line)
        if m:
            task = m.group(1)
            end_time = m.group(3)
            print(f"XX1: import apptask status, task id >{task}<, task_id >{task_id}<")

            task_ctx = root_context.get_context(task, "demo.apptask")
            appfn_ctx = task_ctx.get_context("appfn", "demo.apptask.worker")

            event = Event()
            event.type = "app_in_worker_start"
            event.time = float(m.group(2))
            appfn_ctx.events.append(event)

            event = Event()
            event.type = "app_in_worker_end"
            event.time = float(m.group(3))
            appfn_ctx.events.append(event)


def import_endpoint(known_task_uuids, root_context):

    # 1647999545.795641 2022-03-23 01:39:05 INFO MainProcess-17 TASK_PULL_THREAD-140023248713472 funcx_endpoint.endpoint.interchange:316 _task_puller_loop Received task: 98008dc1-47c6-4244-9e1d-c51021906924

    re_endpoint_interchange_received = re.compile('([0-9.]*) .* _task_puller_loop Received task: ([^ ]*)$')

    # 1647999538.016503 2022-03-23 01:38:58 INFO Executor-Interchange-29 TASK_PULL_THREAD-140023489533696 funcx_endpoint.executors.high_throughput.interchange:472 migrate_tasks_to_internal Received task: 8a0f68ce-eb11-4e2a-80ef-63f487a113a7

    re_executor_interchange_received = re.compile('([0-9.]*) .* migrate_tasks_to_internal Received task: ([^ ]*)$')

    # 1648000349.533943 2022-03-23 01:52:29 DEBUG Executor-Interchange-29 MainThread-140023619893056 funcx_endpoint.executors.high_throughput.interchange:928 start Task: 98008dc1-47c6-4244-9e1d-c51021906924 is now WAITING_FOR_LAUNCH

    re_waiting_for_launch = re.compile('([0-9.]*) .* start Task: ([^ ]*) is now WAITING_FOR_LAUNCH$')

    with open("endpoint.log", "r") as f:
        for l in f:
            m = re_endpoint_interchange_received.match(l)
            if m:
                task_uuid = UUID(m.group(2).strip())
                t = float(m.group(1))
                if task_uuid in known_task_uuids:
                    main_uuid_ctx = root_context.get_context(task_uuid, "funcx.cloudwatch.task")
                    ctx = main_uuid_ctx.get_context("endpoint", "funcx.endpoint")
                    
                    event = Event()
                    event.type = "endpoint_interchange.received"
                    event.time = t
                    ctx.events.append(event)

            m = re_executor_interchange_received.match(l)
            if m:
                task_uuid = UUID(m.group(2).strip())
                t = float(m.group(1))
                if task_uuid in known_task_uuids:
                    main_uuid_ctx = root_context.get_context(task_uuid, "funcx.cloudwatch.task")
                    ctx = main_uuid_ctx.get_context("endpoint", "funcx.endpoint")
                    
                    event = Event()
                    event.type = "executor_interchange.received"
                    event.time = t
                    ctx.events.append(event)

            m = re_waiting_for_launch.match(l)
            if m:
                task_uuid = UUID(m.group(2).strip())
                t = float(m.group(1))
                if task_uuid in known_task_uuids:
                    main_uuid_ctx = root_context.get_context(task_uuid, "funcx.cloudwatch.task")
                    ctx = main_uuid_ctx.get_context("endpoint", "funcx.endpoint")
                    
                    event = Event()
                    event.type = "executor_interchange.WAITING_FOR_LAUNCH"
                    event.time = t
                    ctx.events.append(event)






if source_central == "cloudwatch":
    cloudwatch_ctx = import_cloudwatch(known_task_uuids, root_context)
elif source_central == "file":
    cloudwatch_ctx = import_file(known_task_uuids, root_context)


if source_endpoint:
    endpoint_ctx = import_endpoint(known_task_uuids, root_context)


print(root_context)

print("root context dump:")
root_context.dump()
print("end root context dump")

# =====
ctxs = root_context.subcontexts_by_type("demo.apptask")

def filter_poll_start(ctx):
  new_ctx = Context()
  for e in ctx.events:
    if e.type != "POLL_START":
      new_ctx.events.append(e)
  return new_ctx

ctxs = [filter_poll_start(c) for c in ctxs]

client_colour_states={"SUBMIT": "#FF0000",
               "SUBMIT_POST": "#FF7777",
               # These only manifest when polling, not using ws
               # "POLL_END_PENDING_running": "#FF00FF",
               # "POLL_END_PENDING_waiting-for-launch": "#0000FF",
               # "POLL_END_PENDING_waiting-for-nodes": "#00FFFF",
               "appfn.app_in_worker_start": "#7777FF",
               "appfn.app_in_worker_end": "#007700",
               "POLL_START": "#FFFF00",
               "POLL_END_COMPLETE": "#00FF00"
               }
plot_context_streamgraph(ctxs, "funcx-client-view", client_colour_states)

# ====
# plot of how many tasks are in app_in_worker_start
# i.e. "running" according to my app code

apptask_ctxs = root_context.subcontexts_by_type("demo.apptask")

ctxs = []
for c in apptask_ctxs:
    ctxs += c.subcontexts_by_type("demo.apptask.worker")

appfn_colour_states={
               "app_in_worker_start": "#7777FF",
               "app_in_worker_end": None,
               }
plot_context_streamgraph(ctxs, "funcx-appfn-view-running", appfn_colour_states)

# ====

cloudwatch_colour_states = {"funcx_web_service-user_fetched": "#77FF22",
                            "funcx_forwarder.forwarder-result_enqueued": "#00EE00",
                            "funcx_web_service-received": "#FF7777",
                            "funcx_forwarder.forwarder-dispatched_to_endpoint": "#00FFFF"
                           }


if source_central:
  plot_context_streamgraph(cloudwatch_ctx.subcontexts_by_type("funcx.cloudwatch.task"), "funcx-cloudwatch-view.png", cloudwatch_colour_states)


# plot a streamgraph of all known state transitions, collapsed from all subcontexts

ctxs = root_context.subcontexts_by_type("demo.apptask")

def collapse_ctx(initial_ctx):
    new_ctx = Context()
    absorb_ctx_events(new_ctx, initial_ctx)
    return new_ctx

def absorb_ctx_events(new_ctx, initial_ctx, prefix=""):
    for e in initial_ctx.events:
        if e.type == "SUBMIT_POST":
            continue
        new_e = Event()
        new_e.type = prefix + e.type
        new_e.time = e.time
        new_ctx.events.append(new_e)
    for (name, sub_ctx) in initial_ctx.subcontexts_dict.items():
        absorb_ctx_events(new_ctx, sub_ctx, prefix=prefix+name+".")

# replace each context with a recursively flattened set of events
collapsed_ctxs = [collapse_ctx(ctx) for ctx in ctxs]

collapsed_colour_states = {}
collapsed_colour_states.update({"SUBMIT": "#FF0000"})
collapsed_colour_states.update({"cloudwatch.funcx_web_service-received": "#550000"})
collapsed_colour_states.update({"cloudwatch.funcx_forwarder.forwarder-dispatched_to_endpoint": "#550000"})
collapsed_colour_states.update({"cloudwatch.endpoint.endpoint_interchange.received": "#FFFF00"})
collapsed_colour_states.update({"cloudwatch.endpoint.executor_interchange.received": "#FFAA00"})
collapsed_colour_states.update({"cloudwatch.endpoint.executor_interchange.WAITING_FOR_LAUNCH": "#AAFF00"})
collapsed_colour_states.update({"cloudwatch.times.execution_start": "#5555FF"})
collapsed_colour_states.update({"appfn.app_in_worker_start": "#0000FF"})
collapsed_colour_states.update({"appfn.app_in_worker_end": "#7777FF"})
collapsed_colour_states.update({"cloudwatch.times.execution_end": "#5555FF"})
collapsed_colour_states.update({"cloudwatch.funcx_websocket_service.server-dispatched_to_user": "#33DD33"})
collapsed_colour_states.update({"POLL_END_COMPLETE": "#00FF00"})

plot_context_streamgraph(collapsed_ctxs, "funcx-collapsed-contexts.png", collapsed_colour_states)

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

"""
def scan_context_for_result_to_fetch_duration(ctx):
  events = ctx.events
  enqueued = [e for e in events if e.type == "funcx_forwarder.forwarder-result_enqueued"]
  fetched = [e for e in events if e.type == "funcx_web_service-user_fetched"]
  if len(enqueued) != 1 or len(fetched) != 1:
    raise ValueError("Task does not have correct states for this plot")
    return []
  return [ fetched[0].time - enqueued[0].time ]

ctxs = cloudwatch_ctx.subcontexts_by_type("funcx.cloudwatch.task")
ctx_durations = [scan_context_for_result_to_fetch_duration(c) for c in ctxs ]

durations= []
for d in ctx_durations:
  durations.extend(d)

xs = durations
print(xs)


fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
plt.title("Result enqueued to user fetched duration")
hist, bins, _ = ax.hist(xs, bins=100)

plt.savefig("funcx-cloudwatch-enqueued-to-fetched-histo.png")
"""

if source_central:
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
  ctx_durations = [scan_context_for_enqueued_to_client_completed_duration(c) for c in ctxs ]

  durations= []
  for d in ctx_durations:
    durations.extend(d)

  xs = durations
  print(xs)


  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  plt.title("Result enqueued to user fetched duration")
  hist, bins, _ = ax.hist(xs, bins=100)

  plt.savefig("funcx-cloudwatch-enqueued-to-client-completed-histo.png")


# histogram of user side task completion, and web service funcx_web_service-user_fetched"

"""
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
ctx_durations = [scan_context_for_user_fetched_to_client_completed_duration(c) for c in ctxs ]

durations= []
for d in ctx_durations:
  durations.extend(d)

xs = durations
print(xs)


fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
plt.title("Web service user fetched to client side completed, duration")
hist, bins, _ = ax.hist(xs, bins=100)

plt.savefig("funcx-cloudwatch-user-fetched-to-client-completed-histo.png")
"""

# histogram of task durations according to app level stuff

def context_to_app_reported_duration(ctx):
  subctxs = ctx.subcontexts_by_type("demo.apptask.worker")
  assert len(subctxs) == 1, f"Length of worker contexts list: {len(subctxs)}, list is {subctxs}"
  subctx = subctxs[0]

  assert len(subctx.events) == 2

  start = [e.time for e in subctx.events if e.type == "app_in_worker_start"][0]
  end = [e.time for e in subctx.events if e.type == "app_in_worker_end"][0]

  return (end - start)

durations = [context_to_app_reported_duration(ctx) for ctx in root_context.subcontexts_by_type("demo.apptask")]

fig = plt.figure()
ax = fig.add_subplot(1, 1, 1)
plt.title("Task duration according to app's own worker side logging / seconds")
hist, bins, _ = ax.hist(durations, bins=100)

plt.savefig("funcx-duration-app-worker-side.png")


if source_endpoint and source_central:
  # histogram of WAITING_FOR_LAUNCH vs start time
  def scan_context_for_enqueued_to_client_completed_duration(ctx):
    subctx = ctx.subcontexts_by_type("funcx.cloudwatch.task")
    assert len(subctx) == 1
    subctx = subctx[0].subcontexts_by_type("funcx.endpoint")
    assert len(subctx) == 1
    events = subctx[0].events
    enqueued = [e for e in events if e.type == "executor_interchange.WAITING_FOR_LAUNCH"]
    print(events)
    assert len(enqueued) == 1

    subctx = ctx.subcontexts_by_type("funcx.cloudwatch.task")
    assert len(subctx) == 1
    subctx = subctx[0].subcontexts_by_type("funcx.cloudwatch.task.times")
    assert len(subctx) == 1
    events = subctx[0].events
    completed = [e for e in events if e.type == "execution_start"]
    assert len(completed) == 1

    if len(enqueued) != 1 or len(completed) != 1:
      raise ValueError("Task does not have correct states for this plot")
      return []
    return [ completed[0].time - enqueued[0].time ]

  ctxs = root_context.subcontexts_by_type("demo.apptask")
  ctx_durations = [scan_context_for_enqueued_to_client_completed_duration(c) for c in ctxs ]

  durations= []
  for d in ctx_durations:
    durations.extend(d)

  xs = durations
  print(xs)


  fig = plt.figure()
  ax = fig.add_subplot(1, 1, 1)
  plt.title("Time from WAITING_FOR_LAUNCH to execution begins")
  hist, bins, _ = ax.hist(xs, bins=100)

  plt.savefig("funcx-WAITING_FOR_LAUNCH-to-execution-histo.png")


# histogram of task durations according to funcx worker-side, reported to
# central services


if source_central:
 def context_to_app_reported_duration(ctx):
  subctxs = ctx.subcontexts_by_type("funcx.cloudwatch.task")
  assert len(subctxs) == 1
  subctx = subctxs[0]
  subsubctxs = subctx.subcontexts_by_type("funcx.cloudwatch.task.times")
  assert len(subsubctxs) == 1
  subsubctx = subsubctxs[0]
  assert subsubctx is not None

  assert len(subsubctx.events) == 2

  start = [e.time for e in subsubctx.events if e.type == "execution_start"][0]
  end = [e.time for e in subsubctx.events if e.type == "execution_end"][0]

  return (end - start)

 durations = [context_to_app_reported_duration(ctx) for ctx in root_context.subcontexts_by_type("demo.apptask")]

 fig = plt.figure()
 ax = fig.add_subplot(1, 1, 1)
 plt.title("Task duration according to funcx' worker side logging / seconds")
 hist, bins, _ = ax.hist(durations, bins=100)

 plt.savefig("funcx-duration-funcx-worker-side.png")


# box-and-whisker plot of task end states, normalised against in-app recorded completion time
# It might be interesting to see if I can do data-frame based stuff here
#  - eg make a dataframe of the relevant times,
#       then use dataframe operations to do the normalisation
#       rather than implementing it all in loops and more explicit rearrangements?
# its a good form for box and whisker plot?

# for each context, given "accessors" for each time stamp (that is, a function which given a context returns the appropriate event)
# make a dataframe of each context is a row, each column is the time stamp pointed to by an accessor.

contexts = root_context.subcontexts_by_type("demo.apptask")

def context_app_worker_end_time(ctx):
    """Given a demo.apptask context, returnthe app reported worker end time.
    """
    subctxs = ctx.subcontexts_by_type("demo.apptask.worker")
    assert len(subctxs) == 1
    subctx = subctxs[0]

    assert len(subctx.events) == 2

    end = [e.time for e in subctx.events if e.type == "app_in_worker_end"][0]
    # TODO: assert length of this ^ list as 1

    return end

def context_funcx_worker_end_time(ctx):
    """Given a demo.apptask context, return the funcx reported worker end time.
    """

    subctxs = ctx.subcontexts_by_type("funcx.cloudwatch.task")
    assert len(subctxs) == 1
    subctx = subctxs[0]
    subsubctxs = subctx.subcontexts_by_type("funcx.cloudwatch.task.times")
    assert len(subsubctxs) == 1
    subsubctx = subsubctxs[0]
    assert subsubctx is not None

    assert len(subsubctx.events) == 2

    end = [e.time for e in subsubctx.events if e.type == "execution_end"][0]

    return end


def context_client_poll_end_time(ctx):
    """Given a demo.apptask context, returnthe funcx reported worker end time.
    """

    events = ctx.events
    completed = [e for e in events if e.type == "POLL_END_COMPLETE"]
    assert len(completed) == 1
    return completed[0].time

def context_result_enqueued_time(ctx):
  subctx = ctx.subcontexts_by_type("funcx.cloudwatch.task")
  assert len(subctx) == 1
  events = subctx[0].events
  enqueued = [e for e in events if e.type == "funcx_forwarder.forwarder-result_enqueued"]

  if len(enqueued) != 1:
    raise ValueError("Task does not have correct states for this plot")
  return enqueued[0].time


if source_central:
 accessors = [("app_worker_end", context_app_worker_end_time),
              ("funcx_worker_end", context_funcx_worker_end_time),
              ("result_enqueued", context_result_enqueued_time),
              ("client_poll_complete", context_client_poll_end_time)]

 vs = []

 for ctx in contexts:
  v = []
  for n in range(0, len(accessors)):
    accessor = accessors[n][1]
    new_val = accessor(ctx)
    v.append(new_val)
  vs.append(v)

 cols = [a[0] for a in accessors]

 df = pandas.DataFrame(data=vs, columns=cols)

 print(df)

 base = df['app_worker_end']

 for c in df.columns:
  df[c] = df[c] - base

 print(df)

 formatted_labels = [c.replace("_","\n").replace(".","\n") for c in df.columns]

 fig = plt.figure()
 ax = fig.add_subplot(1, 1, 1)
 plt.title("Ending events, normalised against app worker end / seconds")
 ax.boxplot(df, vert=False, labels=formatted_labels)

 plt.savefig("funcx-ending-whisker.png")

 fig = plt.figure()
 ax = fig.add_subplot(1, 1, 1)
 plt.title("Ending events, normalised against app worker end / seconds")
 ax.boxplot(df, vert=False, labels=formatted_labels, showfliers=False)

 plt.savefig("funcx-ending-whisker-no-outliers.png")


print("end of some_runs_review")
