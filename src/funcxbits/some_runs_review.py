import re

from dnpc.structures import Context, Event
from dnpc.plots import plot_context_streamgraph

# this is specifically aimed at importing the log generated
# by funcxbits.do_some_runs

# import 'do_some_runs.log'

root_context = Context.new_root_context()

print("start import")
with open("do_some_runs.log", "r") as logfile:
    # 1645194062.952741 2022-02-18 14:21:02,952 dnpc.funcx.demoapp:27 MainProcess(157) MainThread [INFO]  TASK 0 RUN
    re_task = re.compile('([0-9.]*) .*  TASK ([^ ]*) ([^ \n]*).*$')

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
plot_context_streamgraph(ctxs, "funcx1", colour_states)

# TODO: histogram submission time (500)
# TODO: histogram poll time (500 x many)

print(root_context)
print("end import")
