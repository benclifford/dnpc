import datetime
import logging
import matplotlib.pyplot as plt
import random

from dnpc.structures import Context, Event

from typing import Dict, List, Optional, Set, Tuple

logger = logging.getLogger(__name__)

def plot_wq_running_to_parsl_running_histo(db_context):

    all_try_contexts = []

    wf_contexts = db_context.subcontexts_by_type("parsl.workflow")

    for wf_context in wf_contexts:
        task_contexts = wf_context.subcontexts_by_type("parsl.task")
        for task_context in task_contexts:
            try_contexts = task_context.subcontexts_by_type("parsl.try")
            all_try_contexts += try_contexts

    # Now all_try_contexts has all of the try contexts in flattened form.
    # Filter so we only have try contexts which have both a running and a returned event

    filtered_try_contexts = []
    for context in all_try_contexts:
        # logger.info(f"examining try context {context}")
        # flatten event_types into a set
        event_types = set()
        for event in context.events:
            event_types.add(event.type)

        executor_contexts = context.subcontexts_by_type("parsl.try.executor")
        # logger.info(f"context.subcontexts = {context.subcontexts}")
        # logger.info(f"executor_contexts = {executor_contexts}")
        if len(executor_contexts) == 0:
            # raise RuntimeError(f"wrong number of executor contexts: {executor_contexts}") # temp dbg
            # logger.info(f"skipping because no executor_context")
            continue
        elif len(executor_contexts) > 1:
            raise RuntimeError(f"Too many executor contexts: {executor_contexts}") # temp dbg


        pte_context = executor_contexts[0]

        pte_event_types = set()
        for event in pte_context.events:
            pte_event_types.add(event.type)

        # logger.info(f"event_types: {event_types}")
        # logger.info(f"pte_event_types: {pte_event_types}")

        if "running" in event_types and 'RUNNING' in pte_event_types:
            filtered_try_contexts.append(context)
        elif "running" in event_types and 'RUNNING' in pte_event_types:
            raise RuntimeError(f"Got one but not the other: {event_types} {pte_event_types}")

    # now filtered_try_contexts has all the tries with the right timestamp

    # map these into something that can be fed into matplotlib histogram
    xs = []
    for context in filtered_try_contexts:
        # extract running and returned values that we know are here
        running_events = [e for e in context.events if e.type == "running"]
        parsl_running_event = running_events[0]  # we selected based on this event existing so [0] will always exist

        executor_contexts = context.subcontexts_by_type("parsl.try.executor")
        # logger.info(f"executor_contexts = {executor_contexts}")
        assert(len(executor_contexts) == 1)
        pte_context = executor_contexts[0]

        wq_running_events = [e for e in pte_context.events if e.type == "RUNNING"]
        wq_running_event = wq_running_events[0]  # we selected based on this event existing so [0] will always exist

        runtime = parsl_running_event.time - wq_running_event.time

        xs.append(runtime)

    # logger.info(f"histo data for runtime: {xs}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    plt.title("time from wq running to parsl running histogram")

    ax.hist(xs, bins=100)

    plt.savefig("dnpc-wq-running-to_parsl-running-histo.png")


def plot_execute_function_to_parsl_running_histo(db_context):

    all_try_contexts = []

    wf_contexts = db_context.subcontexts_by_type("parsl.workflow")

    for wf_context in wf_contexts:
        task_contexts = wf_context.subcontexts_by_type("parsl.task")
        for task_context in task_contexts:
            try_contexts = task_context.subcontexts_by_type("parsl.try")
            all_try_contexts += try_contexts

    # Now all_try_contexts has all of the try contexts in flattened form.
    # Filter so we only have try contexts which have both a running and a returned event

    filtered_try_contexts = []
    for context in all_try_contexts:
        # logger.info(f"examining try context {context}")
        # flatten event_types into a set
        event_types = set()
        for event in context.events:
            event_types.add(event.type)

        executor_contexts = context.subcontexts_by_type("parsl.try.executor")
        # logger.info(f"context.subcontexts = {context.subcontexts}")
        # logger.info(f"executor_contexts = {executor_contexts}")
        if len(executor_contexts) == 0:
            # raise RuntimeError(f"wrong number of executor contexts: {executor_contexts}") # temp dbg
            # logger.info(f"skipping because no executor_context")
            continue
        elif len(executor_contexts) > 1:
            raise RuntimeError(f"Too many executor contexts: {executor_contexts}") # temp dbg
        epf_contexts = executor_contexts[0].subcontexts_by_type("parsl.wq.exec_parsl_function")

        pte_context = epf_contexts[0]

        pte_event_types = set()
        for event in pte_context.events:
            pte_event_types.add(event.type)

        # logger.info(f"event_types: {event_types}")
        # logger.info(f"pte_event_types: {pte_event_types}")

        if "running" in event_types and 'EXECUTEFUNCTION' in pte_event_types:
            filtered_try_contexts.append(context)
        elif "running" in event_types and 'EXECUTEFUNCTION' in pte_event_types:
            raise RuntimeError(f"Got one but not the other: {event_types} {pte_event_types}")

    # now filtered_try_contexts has all the tries with the right timestamp

    # map these into something that can be fed into matplotlib histogram
    xs = []
    for context in filtered_try_contexts:
        # extract running and returned values that we know are here
        running_events = [e for e in context.events if e.type == "running"]
        parsl_running_event = running_events[0]  # we selected based on this event existing so [0] will always exist

        executor_contexts = context.subcontexts_by_type("parsl.try.executor")
        # logger.info(f"executor_contexts = {executor_contexts}")
        assert(len(executor_contexts) == 1)
        epf_contexts = executor_contexts[0].subcontexts_by_type("parsl.wq.exec_parsl_function")
        assert(len(epf_contexts) == 1)
        pte_context = epf_contexts[0]

        wq_running_events = [e for e in pte_context.events if e.type == "EXECUTEFUNCTION"]
        wq_running_event = wq_running_events[0]  # we selected based on this event existing so [0] will always exist

        runtime = parsl_running_event.time - wq_running_event.time

        xs.append(runtime)

    # logger.info(f"histo data for runtime: {xs}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    plt.title("time from wq running to parsl running histogram")

    ax.hist(xs, bins=100)

    plt.savefig("dnpc-execute_function-to_parsl-running-histo.png")


def plot_tries_runtime_histo_wq(db_context):

    all_try_contexts = []

    for wf_context in db_context.subcontexts_by_type("parsl.workflow"):
        for task_context in wf_context.subcontexts_by_type("parsl.task"):
            for try_context in task_context.subcontexts_by_type("parsl.try"):
                if hasattr(try_context, "parsl_executor") and try_context.parsl_executor == "work_queue":
                    all_try_contexts.append(try_context)

    # Now all_try_contexts has all of the try contexts in flattened form.
    # Filter so we only have try contexts which have both a running and a returned event

    filtered_try_contexts = []
    for context in all_try_contexts:
        # flatten event_types into a set
        event_types = set()
        for event in context.events:
            event_types.add(event.type)

        if "running" in event_types and "returned" in event_types:
            filtered_try_contexts.append(context)

    # now filtered_try_contexts has all the tries with the right timestamp

    # map these into something that can be fed into matplotlib histogram
    xs = []
    for context in filtered_try_contexts:
        # extract running and returned values that we know are here
        running_events = [e for e in context.events if e.type == "running"]
        running_event = running_events[0]  # we selected based on this event existing so [0] will always exist

        returned_events = [e for e in context.events if e.type == "returned"]
        returned_event = returned_events[0]  # we selected based on this event existing so [0] will always exist

        runtime = returned_event.time - running_event.time

        xs.append(runtime)

    # logger.info(f"histo data for runtime: {xs}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    plt.title("try runtime histogram")

    ax.hist(xs, bins=100)

    plt.savefig("dnpc-tries-runtime-histo-wq.png")


def plot_tries_runtime_histo_submit(db_context):

    all_try_contexts = []

    for wf_context in db_context.subcontexts_by_type("parsl.workflow"):
        for task_context in wf_context.subcontexts_by_type("parsl.task"):
            for try_context in task_context.subcontexts_by_type("parsl.try"):
                if hasattr(try_context, "parsl_executor") and try_context.parsl_executor == "submit-node":
                    all_try_contexts.append(try_context)

    # Now all_try_contexts has all of the try contexts in flattened form.
    # Filter so we only have try contexts which have both a running and a returned event

    filtered_try_contexts = []
    for context in all_try_contexts:
        # flatten event_types into a set
        event_types = set()
        for event in context.events:
            event_types.add(event.type)

        if "running" in event_types and "returned" in event_types:
            filtered_try_contexts.append(context)

    # now filtered_try_contexts has all the tries with the right timestamp

    # map these into something that can be fed into matplotlib histogram
    xs = []
    for context in filtered_try_contexts:
        # extract running and returned values that we know are here
        running_events = [e for e in context.events if e.type == "running"]
        running_event = running_events[0]  # we selected based on this event existing so [0] will always exist

        returned_events = [e for e in context.events if e.type == "returned"]
        returned_event = returned_events[0]  # we selected based on this event existing so [0] will always exist

        runtime = returned_event.time - running_event.time

        xs.append(runtime)

    # logger.info(f"histo data for runtime: {xs}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    plt.title("try runtime histogram")

    ax.hist(xs, bins=100)

    plt.savefig("dnpc-tries-runtime-histo-submit.png")


def plot_wq_task_runtime_histo(db_context):

    all_wq_task_contexts = []

    for wf_context in db_context.subcontexts_by_type("parsl.workflow"):
        for task_context in wf_context.subcontexts_by_type("parsl.task"):
            for try_context in task_context.subcontexts_by_type("parsl.try"):
                all_wq_task_contexts += try_context.subcontexts_by_type("parsl.try.executor")

    # Now all_try_contexts has all of the try contexts in flattened form.
    # Filter so we only have try contexts which have both a running and a returned event

    filtered_wq_task_contexts = []
    for context in all_wq_task_contexts:
        # flatten event_types into a set
        event_types = set()
        for event in context.events:
            event_types.add(event.type)

        if "RUNNING" in event_types and "DONE" in event_types:
            filtered_wq_task_contexts.append(context)

    # now filtered_try_contexts has all the tries with the right timestamp

    # map these into something that can be fed into matplotlib histogram
    xs = []
    for context in filtered_wq_task_contexts:
        # extract running and returned values that we know are here
        running_events = [e for e in context.events if e.type == "RUNNING"]
        running_event = running_events[0]  # we selected based on this event existing so [0] will always exist

        returned_events = [e for e in context.events if e.type == "DONE"]
        returned_event = returned_events[0]  # we selected based on this event existing so [0] will always exist

        runtime = returned_event.time - running_event.time

        xs.append(runtime)

    # logger.info(f"histo data for runtime: {xs}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    plt.title("wq task runtime histogram")

    ax.hist(xs, bins=100)

    plt.savefig("dnpc-wq-runtime-histo.png")


def plot_tasks_launched_streamgraph_wq_by_type(db_context):
    """Show a stacked plot of tasks in launched state, for the work_queue executor,
    coloured by the app name. This is to investigate relative queue lengths for
    different apps, to see if queue emptying is changing the qualitative behaviour
    of the run."""

    # get every task
    # then make a new collection of contexts, where every task context is mapped
    # to a new context with a start/end event labelled by its app name.

    launched_by_appname_contexts = []
    appnames = set()

    for wf_context in db_context.subcontexts_by_type("parsl.workflow"):
        for task_context in wf_context.subcontexts_by_type("parsl.task"):

            # select only tasks that we want: that is, tasks that have a try
            # in the wq executor.
            select = False
            for try_subcontext in task_context.subcontexts_by_type("parsl.try"):
                if hasattr(try_subcontext, "parsl_executor") and try_subcontext.parsl_executor == "work_queue":
                    select = True
            if not select:
                continue
                
            if task_context.parsl_func_name == "wrapper": # in the run i'm looking at, wrapped should never appear on this graph
                raise RuntimeError(f"got wrapper in plot for only wq (location 1), {task_context}, parsl task id {task_context.parsl_task_id}")

            # TODO: to deal with retries, this launched event mapping code needs
            # to cope with multiple ->launched->other transitions

            context = Context.new_root_context()
            task_events = []
            # this loop really should only happen once or zero, but there's not
            # nicer syntax for a Maybe rather than a List.
            for state_subcontext in task_context.subcontexts_by_type("parsl.task.states"):
                task_events += state_subcontext.events
          
            launched_events = [e for e in task_events if e.type == "launched"]
            if launched_events == []:
                continue  # task was never launched

            for launched_event in launched_events:  # might be several due to retries
                e = Event()
                e.type = task_context.parsl_func_name
                if e.type == "wrapper": # in the run i'm looking at, wrapped should never appear on this graph
                    raise RuntimeError("got wrapper in plot for only wq (location 2)")
                e.time = launched_event.time
                context.events.append(e)
                appnames.add(e.type)

                # don't really care what the next event is - just that there was one
                # that took this task out of launched state
                next_events = [e for e in task_events if e.time > launched_event.time]
                if next_events != []: # then don't generate an end event
                    next_events.sort(key=lambda e: e.time)
                    next_event = next_events[0]

                    e = Event()
                    e.type = "beyond_launched"
                    e.time = next_event.time
                    context.events.append(e)
  
            launched_by_appname_contexts.append(context)
            
    # parsl task-level states
    colour_states : Dict[str, Optional[str]]
    colour_states = {}

    # better hope there's enough for the number of apps
    # this code will raise an exception when there are
    # too many apps to colour.
    colour_list = [ "#FF0000",
                    "#00FF00",
                    "#FFFF00",
                    "#0000FF",
                    "#FF00FF",
                    "#00FFFF"]

    for func_name in appnames:
        colour_states[func_name] = colour_list.pop()


    # mute beyond_launched because it dominates over what is in the queue 
    colour_states.update({
        'beyond_launched': None,
    })

    plot_context_streamgraph(launched_by_appname_contexts, "appname-launched-on-wq", state_config=colour_states)



def plot_tasks_running_streamgraph_wq_by_type(db_context):
    """Show a stacked plot of tasks in launched state, for the work_queue executor,
    coloured by the app name. This is to investigate relative queue lengths for
    different apps, to see if queue emptying is changing the qualitative behaviour
    of the run."""

    # get every task
    # then make a new collection of contexts, where every task context is mapped
    # to a new context with a start/end event labelled by its app name.

    running_by_appname_contexts = []
    appnames = set()

    for wf_context in db_context.subcontexts_by_type("parsl.workflow"):
        for task_context in wf_context.subcontexts_by_type("parsl.task"):

            # select only tasks that we want: that is, tasks that have a try
            # in the wq executor.
            select = False
            for try_subcontext in task_context.subcontexts_by_type("parsl.try"):
                if hasattr(try_subcontext, 'parsl_executor') and try_subcontext.parsl_executor == "work_queue":
                    select = True
            if not select:
                continue
                

            context = Context.new_root_context()

            # find the allocation of this task to a worker node
            try_subcontexts = task_context.subcontexts_by_type("parsl.try")
            if try_subcontexts == []:
                continue  # skip no-tries case


            for try_context in try_subcontexts:
                try_context =  try_subcontexts[0]

                wq_subcontexts = try_context.subcontexts_by_type("parsl.try.executor")
                if len(wq_subcontexts) != 1:
                    continue # skip tries with missing wq events
                wq_context = wq_subcontexts[0]
            
                # find the wq RUNNING event 
               
                wq_running_events = [e for e in wq_context.events if e.type == "RUNNING"]

                if len(wq_running_events) == 1:
                    # skip this event with missing wq RUNNING

                    wq_running_event = wq_running_events[0]

                    e = Event()
                    e.type = "Worker prep"
                    e.time = wq_running_event.time
                    context.events.append(e)

            # find the parsl running and end of running events:

            # this loop really should only happen once or zero, but there's not
            # nicer syntax for a Maybe rather than a List.
            task_events = []
            for state_subcontext in task_context.subcontexts_by_type("parsl.task.states"):
                task_events += state_subcontext.events
          
            start_events = [e for e in task_events if e.type == "running"]
            if start_events == []:
                continue  # task was never launched

            for start_event in start_events:

                e = Event()
                e.type = task_context.parsl_func_name
                e.time = start_event.time
                context.events.append(e)
                appnames.add(e.type)

                next_events = [e for e in task_events if e.time > start_event.time]
                if next_events != []: # then don't generate an end event
                    next_events.sort(key=lambda e: e.time)
                    next_event = next_events[0]

                    e = Event()
                    e.type = "beyond_running"
                    e.time = next_event.time
                    context.events.append(e)
  
            running_by_appname_contexts.append(context)
            
    # parsl task-level states
    colour_states: Dict[str, Optional[str]]
    colour_states = {
        'Worker prep': '#777777'
    }

    # better hope there's enough for the number of apps
    # this code will raise an exception when there are
    # too many apps to colour.
    colour_list = [ "#FF0000",
                    "#00FF00",
                    "#FFFF00",
                    "#0000FF",
                    "#FF00FF",
                    "#00FFFF"]

    for func_name in appnames:
        colour_states[func_name] = colour_list.pop()


    # mute beyond_running because it dominates (total tasks vs running now don't work
    # well on the same y axis) 
    colour_states.update({
        'beyond_running': None,
    })

    plot_context_streamgraph(running_by_appname_contexts, "appname-running-on-wq", state_config=colour_states)


def plot_tasks_running_streamgraph_wq_by_type_mem_weighted(db_context):
    """Show a stacked plot of tasks in launched state, for the work_queue executor,
    coloured by the app name. This is to investigate relative queue lengths for
    different apps, to see if queue emptying is changing the qualitative behaviour
    of the run."""

    # get every task
    # then make a new collection of contexts, where every task context is mapped
    # to a new context with a start/end event labelled by its app name.

    running_by_appname_contexts = []
    appnames = set()

    for wf_context in db_context.subcontexts_by_type("parsl.workflow"):
        for task_context in wf_context.subcontexts_by_type("parsl.task"):

            # select only tasks that we want: that is, tasks that have a try
            # in the wq executor.
            select = False
            for try_subcontext in task_context.subcontexts_by_type("parsl.try"):
                if hasattr(try_subcontext, 'parsl_executor') and try_subcontext.parsl_executor == "work_queue":
                    select = True
            if not select:
                continue
                

            context = Context.new_root_context()

            # find the allocation of this task to a worker node
            try_subcontexts = task_context.subcontexts_by_type("parsl.try")
            if try_subcontexts == []:
                continue  # skip no-tries case


            for try_context in try_subcontexts:
                try_context =  try_subcontexts[0]

                wq_subcontexts = try_context.subcontexts_by_type("parsl.try.executor")
                if len(wq_subcontexts) != 1:
                    continue # skip tries with missing wq events
                wq_context = wq_subcontexts[0]

                if hasattr(wq_context, "wq_resource_memory"):
                    context.plot_weight = wq_context.wq_resource_memory
            
                # find the wq RUNNING event 
               
                wq_running_events = [e for e in wq_context.events if e.type == "RUNNING"]

                if len(wq_running_events) == 1:
                    # skip this event with missing wq RUNNING

                    wq_running_event = wq_running_events[0]

                    e = Event()
                    e.type = "Worker prep"
                    e.time = wq_running_event.time
                    context.events.append(e)

            # find the parsl running and end of running events:

            # this loop really should only happen once or zero, but there's not
            # nicer syntax for a Maybe rather than a List.
            task_events = []
            for state_subcontext in task_context.subcontexts_by_type("parsl.task.states"):
                task_events += state_subcontext.events
          
            start_events = [e for e in task_events if e.type == "running"]
            if start_events == []:
                continue  # task was never launched

            for start_event in start_events:

                e = Event()
                e.type = task_context.parsl_func_name
                e.time = start_event.time
                context.events.append(e)
                appnames.add(e.type)

                next_events = [e for e in task_events if e.time > start_event.time]
                if next_events != []: # then don't generate an end event
                    next_events.sort(key=lambda e: e.time)
                    next_event = next_events[0]

                    e = Event()
                    e.type = "beyond_running"
                    e.time = next_event.time
                    context.events.append(e)
  
            running_by_appname_contexts.append(context)
            
    # parsl task-level states
    colour_states: Dict[str, Optional[str]]
    colour_states = {
        'Worker prep': '#777777'
    }

    # better hope there's enough for the number of apps
    # this code will raise an exception when there are
    # too many apps to colour.
    colour_list = [ "#FF0000",
                    "#00FF00",
                    "#FFFF00",
                    "#0000FF",
                    "#FF00FF",
                    "#00FFFF"]

    for func_name in appnames:
        colour_states[func_name] = colour_list.pop()


    # mute beyond_running because it dominates (total tasks vs running now don't work
    # well on the same y axis) 
    colour_states.update({
        'beyond_running': None,
    })

    plot_context_streamgraph(running_by_appname_contexts, "appname-running-on-wq-mem-weighted", state_config=colour_states)


def plot_tries_cumul(db_context):
    """Given a DB context, plot cumulative state transitions of all tries of all tasks of all workflows"""

    # pivot from events being grouped by context, to being
    # grouped by event type

    all_subcontext_events = []

    for wf_context in db_context.subcontexts_by_type("parsl.workflow"):
        for task_context in wf_context.subcontexts_by_type("parsl.task"):
            for try_subcontext in task_context.subcontexts_by_type("parsl.try"):
                all_subcontext_events += try_subcontext.events

    # logger.info(f"all subcontext events: {all_subcontext_events}")

    event_types = set()

    for event in all_subcontext_events:
        event_types.add(event.type)

    logger.info(f"all event types: {event_types}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    for event_type in event_types:

        x = []
        y = []
        these_events = [event for event in all_subcontext_events if event.type == event_type]

        these_events.sort(key=lambda e: e.time)

        n = 0
        for event in these_events:
            x.append(event.time)
            y.append(n)
            n += 1
            x.append(event.time)
            y.append(n)

        # logger.info(f"will plot event {event_type} with x={x} and y={y}")
        ax.plot(x, y, label=f"{event_type}")

    ax.legend()
    plt.title("cumulative monitoring.db task events by time")

    plt.savefig("dnpc-tries-cumul.png")


def plot_tasks_summary_cumul(db_context):
    """Given a DB context, plot cumulative state transitions of all tasks of all workflows"""

    # pivot from events being grouped by context, to being
    # grouped by event type

    all_subcontext_events = []

    for wf_context in db_context.subcontexts_by_type("parsl.workflow"):
        for task_context in wf_context.subcontexts_by_type("parsl.task"):
            for state_subcontext in task_context.subcontexts_by_type("parsl.task.summary"):
                all_subcontext_events += state_subcontext.events

    # logger.info(f"all subcontext events: {all_subcontext_events}")

    event_types = set()

    for event in all_subcontext_events:
        event_types.add(event.type)

    logger.info(f"all event types: {event_types}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    for event_type in event_types:

        x = []
        y = []
        these_events = [event for event in all_subcontext_events if event.type == event_type]

        these_events.sort(key=lambda e: e.time)

        n = 0
        for event in these_events:
            x.append(event.time)
            y.append(n)
            n += 1
            x.append(event.time)
            y.append(n)

        # logger.info(f"will plot event {event_type} with x={x} and y={y}")
        ax.plot(x, y, label=f"{event_type}")

    ax.legend()
    plt.title("cumulative monitoring.db task events by time")

    plt.savefig("dnpc-tasks-summary-cumul.png")


def plot_tasks_status_cumul(db_context):
    """Given a DB context, plot cumulative state transitions of all tasks of all workflows"""

    # pivot from events being grouped by context, to being
    # grouped by event type

    all_subcontext_events = []

    for wf_context in db_context.subcontexts_by_type("parsl.workflow"):
        for task_context in wf_context.subcontexts_by_type("parsl.task"):
            for state_subcontext in task_context.subcontexts_by_type("parsl.task.states"):
                all_subcontext_events += state_subcontext.events

    # logger.info(f"all subcontext events: {all_subcontext_events}")

    event_types = set()

    for event in all_subcontext_events:
        event_types.add(event.type)

    logger.info(f"all event types: {event_types}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    for event_type in event_types:

        x = []
        y = []
        these_events = [event for event in all_subcontext_events if event.type == event_type]

        these_events.sort(key=lambda e: e.time)

        n = 0
        for event in these_events:
            x.append(event.time)
            y.append(n)
            n += 1
            x.append(event.time)
            y.append(n)

        # logger.info(f"will plot event {event_type} with x={x} and y={y}")
        ax.plot(x, y, label=f"{event_type}")

    ax.legend()
    plt.title("cumulative monitoring.db task events by time")

    plt.savefig("dnpc-tasks-status-cumul.png")


def plot_tasks_status_streamgraph_wq(db_context):
    """status stream of tasks sent to the wq executor"""

    all_state_subcontexts = set()

    for wf_context in db_context.subcontexts_by_type("parsl.workflow"):
        logger.info(f"number of task subcontexts: {len(wf_context.subcontexts_by_type('parsl.task'))}")
        for task_context in wf_context.subcontexts_by_type("parsl.task"):

            # select task context if it has a try subcontext with a parsl_executor
            # attribute of 'submit-node'

            try_subcontexts = task_context.subcontexts_by_type("parsl.try")
            for try_subcontext in try_subcontexts:
                if not hasattr(try_subcontext, "parsl_executor"):
                    logger.info(f"try subcontext has no executor: {try_subcontext}, task_context = {task_context}, try_context subcontexts {try_subcontext.subcontexts}")
                elif try_subcontext.parsl_executor == "work_queue":

                    all_state_subcontexts.update(task_context.subcontexts_by_type("parsl.task.states"))
                    break

    # parsl task-level states
    colour_states = {
        'pending': "#222222",
        'launched': "#000055",
        'running': "#77FFFF",
        'running_ended': "77FF77",
        'exec_done': "#005500"

    }

    plot_context_streamgraph(all_state_subcontexts, "tasks-status-wq", state_config=colour_states)


def plot_tasks_status_streamgraph_submit(db_context):
    """status stream of tasks sent to the submit executor"""

    all_state_subcontexts = set()

    for wf_context in db_context.subcontexts_by_type("parsl.workflow"):
        logger.info(f"number of task subcontexts: {len(wf_context.subcontexts_by_type('parsl.task'))}")
        for task_context in wf_context.subcontexts_by_type("parsl.task"):

            # select task context if it has a try subcontext with a parsl_executor
            # attribute of 'submit-node'

            try_subcontexts = task_context.subcontexts_by_type("parsl.try")
            for try_subcontext in try_subcontexts:
                if not hasattr(try_subcontext, "parsl_executor"):
                    logger.info(f"try subcontext has no executor: {try_subcontext}, task_context = {task_context}, try_context subcontexts {try_subcontext.subcontexts}")
                elif try_subcontext.parsl_executor == "submit-node":

                    all_state_subcontexts.update(task_context.subcontexts_by_type("parsl.task.states"))
                    break

    # parsl task-level states
    colour_states = {
        'pending': "#222222",
        'launched': "#000055",
        'running': "#77FFFF",
        'running_ended': "77FF77",
        'exec_done': "#005500"

    }

    plot_context_streamgraph(all_state_subcontexts, "tasks-status-submit", state_config=colour_states)


def plot_tasks_status_streamgraph(db_context):

    all_state_subcontexts = set()

    for wf_context in db_context.subcontexts_by_type("parsl.workflow"):
        for task_context in wf_context.subcontexts_by_type("parsl.task"):
            all_state_subcontexts.update(task_context.subcontexts_by_type("parsl.task.states"))

    # parsl task-level states
    colour_states = {
        'pending': "#222222",
        'launched': "#000055",
        'running': "#77FFFF",
        'running_ended': "77FF77",
        'exec_done': "#005500"

    }

    plot_context_streamgraph(all_state_subcontexts, "tasks-status", state_config=colour_states)


def plot_task_running_event_stacked_and_streamgraph_wq(db_context):

    logger.info("starting stacked_and_streamgraph unified plot code")
    all_state_subcontexts = set()

    for wf_context in db_context.subcontexts_by_type("parsl.workflow"):
        for task_context in wf_context.subcontexts_by_type("parsl.task"):
            this_task_contexts = set()
            for try_subcontext in task_context.subcontexts_by_type("parsl.try"):
                wq_contexts = try_subcontext.subcontexts_by_type("parsl.try.executor")
                this_task_contexts.update(wq_contexts)
                for wq_subcontext in wq_contexts:
                    if hasattr(try_subcontext, "parsl_executor") and try_subcontext.parsl_executor == "work_queue":
                        this_task_contexts.update(wq_subcontext.subcontexts)

            state_contexts = task_context.subcontexts_by_type("parsl.task.states")

            this_task_contexts.update(state_contexts)
            collapsed_context = Context.new_root_context()
            for c in this_task_contexts:
                for e in c.events:
                    new_event = Event()
                    new_event.time = e.time
                    new_event.type = c.type + "." + e.type
                    collapsed_context.events.append(new_event)
                
            collapsed_context.events.sort(key=lambda e: e.time)

            # allowed_end_states = ['exec_done', 'failed', 'memo_done', 'dep_fail','DONE', 'running_ended', 'pending']
            # ignore states that don't use a worker
            config_states = {
                # before
                'parsl.task.states.pending': None,
                'parsl.task.states.launched': None,
                'parsl.try.executor.WAITING': None,

                # during
                'parsl.try.executor.RUNNING': "#FF0000",
                'parsl.wq.exec_parsl_function.START': "#00FF00",
                'parsl.wq.exec_parsl_function.POSTIMPORT': "#FFFF00",
                'parsl.wq.exec_parsl_function.MAINSTART': "#0000FF",
                'parsl.wq.exec_parsl_function.LOADFUNCTION': "#FF00FF",
                'parsl.wq.exec_parsl_function.EXECUTEFUNCTION': "#777777",

                'parsl.task.states.running': "#FF6600",


                # starting to end
                'parsl.task.states.running_ended': "#806680",
                'parsl.wq.exec_parsl_function.DUMP': "#809980",
                'parsl.wq.exec_parsl_function.DONE': "#80FF80",
                'parsl.task.states.joining': None,
                'parsl.try.executor.WAITING_RETRIEVAL': "#801180",
                'parsl.try.executor.RETRIEVED': "#802280",
                'parsl.try.executor.DONE': None,

                # after
 
                'parsl.task.states.dep_fail': None, # "#FF8888",
                'parsl.task.states.failed': None, # "#FF0000",
                'parsl.task.states.exec_done': None, # "#00FF00",
                'parsl.task.states.memo_done': None, # "#88FF88",
            }

            all_except_done_config_states = {
                # before
                'parsl.task.states.pending': "#669999",
                'parsl.task.states.launched': "#007777",
                'parsl.try.executor.WAITING': "#006666",

                # during
                'parsl.try.executor.RUNNING': "#FF22FF",
                'parsl.wq.exec_parsl_function.START': "#FF44FF",
                'parsl.wq.exec_parsl_function.POSTIMPORT': "#FF55FF",
                'parsl.wq.exec_parsl_function.MAINSTART': "#FF66FF",
                'parsl.wq.exec_parsl_function.LOADFUNCTION': "#FF77FF",
                'parsl.wq.exec_parsl_function.EXECUTEFUNCTION': "#FF88FF",

                'parsl.task.states.running': "#FF6600",


                # starting to end
                'parsl.task.states.running_ended': "#806680",
                'parsl.wq.exec_parsl_function.DUMP': "#809980",
                'parsl.wq.exec_parsl_function.DONE': "#80FF80",
                'parsl.task.states.joining': "#800080",
                'parsl.try.executor.WAITING_RETRIEVAL': "#801180",
                'parsl.try.executor.RETRIEVED': "#802280",
                'parsl.try.executor.DONE': None, # this is happening after exec_done in a substantial number of cases so mute it

                # after
 
                'parsl.task.states.dep_fail': None, # "#FF8888",
                'parsl.task.states.failed': None, # "#FF0000",
                'parsl.task.states.exec_done': None, # "#00FF00",
                'parsl.task.states.memo_done': None, # "#88FF88",
            }
            # assert collapsed_context.events[-1].type in allowed_end_states, \
            #    f"Bad final end state for event list {collapsed_context.events}"
            all_state_subcontexts.add(collapsed_context)

            # logger.info(f"BENC context events: {collapsed_context.events}")

    logger.info("post-shared-processing - starting plot")
    plot_context_streamgraph(all_state_subcontexts, "tasks-running-event-wq", state_config=config_states)
    logger.info("starting stacked_and_streamgraph unified plot code")


def plot_context_streamgraph(all_state_subcontexts, filebase, state_config={}):


    logger.info("plot_context_streamgraph: starting")

    # TODO: factor this out... it's specific to the particular components being monitored.
    # The state sequence is not automatable so much:
    # for a particular source its specific to that source, and in the case of an
    # integration of sources, that's more complicated - the "integrator" should
    # specify some model, perhaps?
    # as part of the state_config parameter
    implicit_state_sequence = ["running", "exec_done"] + ["parsl.wq.exec_parsl_function.EXECUTEFUNCTION", "parsl.task.states.running", "parsl.task.states.running_ended", "parsl.task.states.exec_done"]
    # given an event, disambiguate its event ordering wrt other events
    # which might happen in the same instant, by applying domain specific
    # knowledge about sort orders.
    def sortkey(e):
        # convert e into an integer in the list of sort key positions.
        if e.type in implicit_state_sequence:
            return implicit_state_sequence.index(e.type)
        else:
            return -1

    all_subcontext_events = []

    logger.info("plot_context_streamgraph: collecting contexts")
    for context in all_state_subcontexts:
        all_subcontext_events += context.events

    # logger.info(f"all subcontext events: {all_subcontext_events}")

    event_types = set()

    logger.info("plot_context_streamgraph: collecting event types")
    for event in all_subcontext_events:
        event_types.add(event.type)

    # logger.info(f"Stack/streamgraph: all event types: {event_types}")

    # now generate a different stream of events, to be used for plotting:
    # for each task,
    # the first event increases the event type
    # subsequent events increase the event type and decrease the former
    # event type

    plot_events: Dict[str, List[Tuple[float, int]]]
    plot_events = {}
    for t in event_types:
        plot_events[t] = []

    logger.info("plot_context_streamgraph: collecting events")
    for s in all_state_subcontexts:
        if len(s.events) == 0:
            continue

        # this probably should be passed as a paramter to this plotting
        # function, but for testing, try it out hard-coded:

        these_events = [e for e in s.events]  # copy so we can mutate safely
        these_events.sort(key=lambda e: (e.time, sortkey(e)))

        # this code was investigating why i was getting a lot of running ended...
        # leading to me doing more interesting stuff with the above sort keys
        # if these_events[-1].type == "running_ended":
        #     raise RuntimeError(f"Events list ended with running_ended: {these_events}")
        # if these_events[-1].type == "parsl.task.states.running_ended":
        #    raise RuntimeError(f"Events list ended with parsl.task.states.running_ended: {these_events}")

        if hasattr(s, "plot_weight"):
            weight = s.plot_weight
        else:
            weight = 1

        plot_events[these_events[0].type].append((these_events[0].time, weight))
        prev_event_type = these_events[0].type
        for e in these_events[1:]:
            plot_events[e.type].append((e.time, weight))
            plot_events[prev_event_type].append((e.time, -weight))
            prev_event_type = e.type
        # if prev_event_type != "exec_done":
        #    raise RuntimeError(f"did not end on exec_done: {prev_event_type}, {these_events}")

    # TODO: now we have per-event type data series, with mismatching x axes
    # for each of those data series, align the x axes by duplicating entries
    # to ensure the x axis is fully populated

    logger.info("plot_context_streamgraph: collecting x axis values")
    canonical_x_axis_set: Set[float]
    canonical_x_axis_set = set()
    for t in event_types:
        these_x = [e[0] for e in plot_events[t]]
        # logger.info(f"these_x = {these_x}")
        logger.info(f"event type {t} adding {len(these_x)} timestamps")
        logger.info(f"size before update: {len(canonical_x_axis_set)}")
        canonical_x_axis_set.update(these_x)
        logger.info(f"size after update: {len(canonical_x_axis_set)}")

    canonical_x_axis = list(canonical_x_axis_set)
    canonical_x_axis.sort()

    if len(canonical_x_axis) == 0:
        raise ValueError("This plot has no x axis values (so no events, probably)")

    ys = []
    labels = []
    colors = []

    new_event_types = list(state_config.keys())

    remaining = list(event_types.difference(new_event_types))
    remaining.sort()
    new_event_types += remaining
  
    # matplotlib color cycle number
    c_n = 0

    logger.info("plot_context_streamgraph: iterating over event types")
    for event_type in new_event_types:
        logger.info(f"plot_context_streamgraph: {event_type}: start")
        config = state_config.get(event_type, -1)
        if config is None:
            logger.info(f"plot_context_streamgraph: {event_type}: skipping as muted")
            continue

        y = []
        these_events = plot_events.get(event_type, [])

        these_events.sort(key=lambda pe: pe[0])

        n = 0

        # this sweep is very expensive and also done once per event type, making it even more expensive
        # its 45s of runtime in an invocation of this function which takes 72s - about 60%
        logger.info(f"plot_context_streamgraph: {event_type}: sweeping x axis")

        these_events_index = 0
        these_events_len = len(these_events)
        for x in canonical_x_axis:

            while these_events_index < these_events_len and these_events[these_events_index][0] == x:
                # these asserts are very expensive, and covered by the
                # assert right after the while loop
                # assert these_events[0][0] in canonical_x_axis_set, "timestamp must be in x axis somewhere"
                # assert these_events[0][0] in canonical_x_axis, "timestamp must be in x axis list somewhere"
                n += these_events[these_events_index][1]
                these_events_index += 1
                # these_events = these_events[1:]   # I suspect this is very expensive, recreating a new list each time...
                                                    # so instead of this functional style, perhaps use an index into the list

            assert these_events_index == these_events_len or these_events[these_events_index][0] > x, "Next event must be in future"
            y.append(n)

        logger.info(f"plot_context_streamgraph: {event_type}: done sweeping x axis")
        # logger.info(f"event type {event_type} event list {these_events} generated sequence {y}")

        # we should have used up all of the events for this event type
        assert these_events_index == these_events_len, f"Some events remaining: index {these_events_index} is not length of list {these_events_len}"

        # logger.info(f"will plot event {event_type} with x={x} and y={y}")

        ys.append(y)
        labels.append(event_type)
        if config != -1:
            colors.append(config)
        else:
            colors.append("#777777") # todo something more auto distinct
            c_n += 1
        logger.info(f"plot_context_streamgraph: {event_type}: done")

    x_base = canonical_x_axis[0]
    shifted_x_axis = [x - x_base for x in canonical_x_axis]

    logger.info("plot_context_streamgraph: iterating over baselines")
    for baseline in ['zero', 'wiggle']:
        fig = plt.figure(figsize=(16, 10))
        ax = fig.add_subplot(1, 1, 1)

        ax.stackplot(shifted_x_axis, ys, labels=labels, colors=colors, baseline=baseline)
        ax.legend(loc='upper left')
        plt.title(f"Contexts in each state by time ({baseline} baseline)")

        plt.savefig(filebase+"-"+baseline+".png")
    logger.info("plot_context_streamgraph: done")


def plot_all_task_events_cumul(db_context, filename="dnpc-all-task-events-cumul.png"):
    all_subcontext_events = []

    # TODO: this should maybe use a set for all_subcontext_events:
    # in some cases, there might be multiple routes to the same context,
    # and each context should only be counted once.
    for wf_context in db_context.subcontexts_by_type("parsl.workflow"):
        for task_context in wf_context.subcontexts_by_type("parsl.task"):
            all_subcontext_events += task_context.events

            for try_subcontext in task_context.subcontexts_by_type("parsl.try"):
                all_subcontext_events += try_subcontext.events
                for wq_subcontext in try_subcontext.subcontexts_by_type("parsl.try.executor"):
                    all_subcontext_events += wq_subcontext.events
                    for s in wq_subcontext.subcontexts:
                        all_subcontext_events += s.events

            for state_context in task_context.subcontexts_by_type("parsl.task.states"): 
                all_subcontext_events += state_context.events

    # logger.info(f"all subcontext events: {all_subcontext_events}")

    event_types = set()

    for event in all_subcontext_events:
        event_types.add(event.type)

    logger.info(f"all event types: {event_types}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    for event_type in event_types:

        x = []
        y = []
        these_events = [event for event in all_subcontext_events if event.type == event_type]

        these_events.sort(key=lambda e: e.time)

        n = 0
        for event in these_events:
            x.append(event.time)
            y.append(n)
            n += 1
            x.append(event.time)
            y.append(n)

        # logger.info(f"will plot event {event_type} with x={x} and y={y}")
        ax.plot(x, y, label=f"{event_type}")

    ax.legend()
    plt.title("cumulative task events (parsl/wq/worker) by time")

    plt.savefig(filename)


def plot_wq_parsl_worker_cumul(db_context):

    # pivot from events being grouped by context, to being
    # grouped by event type

    all_subcontext_events = []

    for wq_context in db_context.subcontexts_by_type("parsl.workflow"):
        for task_context in wq_context.subcontexts_by_type("parsl.task"):
            for try_subcontext in task_context.subcontexts_by_type("parsl.try"):
                for wq_subcontext in try_subcontext.subcontexts_by_type("parsl.try.executor"):
                    all_subcontext_events += wq_subcontext.events

    # logger.info(f"all subcontext events: {all_subcontext_events}")

    event_types = set()

    for event in all_subcontext_events:
        event_types.add(event.type)

    logger.info(f"all event types: {event_types}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    for event_type in event_types:

        x = []
        y = []
        these_events = [event for event in all_subcontext_events if event.type == event_type]

        these_events.sort(key=lambda e: e.time)

        n = 0
        for event in these_events:
            x.append(event.time)
            y.append(n)
            n += 1
            x.append(event.time)
            y.append(n)

        # logger.info(f"will plot event {event_type} with x={x} and y={y}")
        ax.plot(x, y, label=f"{event_type}")

    ax.legend()
    plt.title("cumulative wq-parsl worker events by time")

    plt.savefig("dnpc-wq-parsl-worker-cumul.png")


# How does this conversion from spans to graphs match up with feeding in raw gauge data?
def plot_parsl_workflows_cumul(db_context):
    all_subcontext_events = []

    for context in db_context.subcontexts_by_type("parsl.workflow"):
        all_subcontext_events += context.events

    event_types = set()

    for event in all_subcontext_events:
        event_types.add(event.type)

    logger.info(f"This plot contains these event types: {event_types}")

    fig = plt.figure()
    ax = fig.add_subplot(1, 1, 1)

    for event_type in event_types:

        x = []
        y = []
        these_events = [event for event in all_subcontext_events if event.type == event_type]

        these_events.sort(key=lambda e: e.time)

        n = 0
        for event in these_events:
            x.append(datetime.datetime.fromtimestamp(event.time)) # what does datetime know about timezones? the docs for fromtimestamp talk about the "local" timezone
            y.append(n)
            n += 1
            x.append(datetime.datetime.fromtimestamp(event.time))
            y.append(n)

        logger.info(f"will plot event {event_type} with x={x} and y={y}")
        ax.plot(x, y, label=f"{event_type}")

    ax.legend()
    fig.autofmt_xdate()
    plt.title("cumulative monitoring.db workflow events over time")

    plt.savefig("dnpc-workflows-cumul.png")


