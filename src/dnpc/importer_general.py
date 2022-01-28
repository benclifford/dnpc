import datetime
import json
import logging
import os
import re
import sqlite3
import matplotlib.pyplot as plt

from parsl.log_utils import set_stream_logger
from typing import Dict, List, Optional, Tuple


from dnpc.structures import Context, Event

from dnpc.plots import (plot_parsl_workflows_cumul,
    plot_tasks_summary_cumul,
    plot_tasks_status_cumul,
    plot_tries_cumul,
    plot_tries_runtime_histo_submit,
    plot_tries_runtime_histo_wq,
    plot_wq_task_runtime_histo,
    plot_wq_running_to_parsl_running_histo,
    plot_wq_parsl_worker_cumul,
    plot_all_task_events_cumul,
    plot_tasks_status_streamgraph,
    plot_tasks_status_streamgraph_submit,
    plot_tasks_status_streamgraph_wq,
    plot_task_running_event_stacked_and_streamgraph_wq,
    plot_execute_function_to_parsl_running_histo,
    plot_tasks_launched_streamgraph_wq_by_type,
    plot_tasks_running_streamgraph_wq_by_type,
    plot_tasks_running_streamgraph_wq_by_type_mem_weighted
    )


logger = logging.getLogger("dnpc.main")  # __name__ is not package qualified in __main__

def import_workflow_task_tries(base_context: Context, db: sqlite3.Connection, run_id: str, parsl_tz_shift: float) -> None:
    logger.debug(f"Importing tries for all tasks in run {run_id}")

    cur = db.cursor()

    # this fractional seconds replacement for %s comes from (julianday('now') - 2440587.5)*86400.0
    # SELECT (julianday('now') - 2440587.5)*86400.0;

    for row in cur.execute(f"SELECT task_id, try_id, (julianday(task_try_time_launched) - 2440587.5)*86400.0, "
                           f"(julianday(task_try_time_running) - 2440587.5)*86400.0, (julianday(task_try_time_returned) - 2440587.5)*86400.0, "
                           f"task_executor "
                           f"FROM try WHERE run_id = '{run_id}'"):
        task_id = row[0]
        try_id = row[1]

        task_context = base_context.get_context(task_id, "parsl.task")
        try_context = task_context.get_context(try_id, "parsl.try")

        try_context.name = f"Try {try_id} on executor {row[4]} via db importer"

        if row[2]:  # omit this event if it is NULL
            launched_event = Event()
            launched_event.type = "launched"
            launched_event.time = float(row[2]) + parsl_tz_shift
            try_context.events.append(launched_event)

        if row[3]:  # omit this event if it is NULL
            running_event = Event()
            running_event.type = "running"
            running_event.time = float(row[3]) + parsl_tz_shift
            try_context.events.append(running_event)

        if row[4] is not None:
            # then the task returned
            returned_event = Event()
            returned_event.type = "returned"
            returned_event.time = float(row[4]) + parsl_tz_shift
            try_context.events.append(returned_event)

        try_context.parsl_executor = row[5]

    return None


def import_workflow_tasks(base_context: Context, db: sqlite3.Connection, run_id: str, parsl_tz_shift: float) -> None:
    logger.info(f"Importing tasks for workflow {run_id}")

    cur = db.cursor()

    for row in cur.execute(f"SELECT task_id, strftime('%s', task_time_invoked), strftime('%s',task_time_returned), task_func_name FROM task WHERE run_id = '{run_id}'"):
        task_id = row[0]
        task_context = base_context.get_context(task_id, "parsl.task")
        task_context.name = f"Task {task_id}"

        task_context.parsl_func_name = row[3]
        task_context.parsl_task_id = task_id

        summary_context = task_context.get_context("summary", "parsl.task.summary")
        summary_context.name = f"Task {task_id} summary"

        start_event = Event()
        start_event.type = "start"
        start_event.time = float(row[1]) + parsl_tz_shift
        summary_context.events.append(start_event)

        if row[2] is not None:
            # ... then the task was recorded as finished
            end_event = Event()
            end_event.type = "end"
            end_event.time = float(row[2]) + parsl_tz_shift
            summary_context.events.append(end_event)

        state_context = task_context.get_context("states", "parsl.task.states")
        state_context.name = f"Task {task_id} states"

        state_cur = db.cursor()
        for state_row in state_cur.execute(f"SELECT task_status_name, (julianday(timestamp) - 2440587.5)*86400.0 "
                                           f"FROM status WHERE run_id = '{run_id}' AND task_id = '{task_id}'"):
            start_event = Event()
            start_event.type = state_row[0]
            start_event.time = float(state_row[1]) + parsl_tz_shift
            state_context.events.append(start_event)

    import_workflow_task_tries(base_context, db, run_id, parsl_tz_shift)

    return None


def import_parsl_log(base_context: Context, rundir: str) -> None:
    logger.info("Importing parsl.log")

    # wq_context = base_context.get_context("WorkQueueExecutor", "parsl.executor")
    #    wq_task_context = wq_context.get_context(wqe_task_id, "parsl.try.executor")
    #    epf_context = wq_task_context.get_context("epf", "parsl.wq.exec_parsl_function")

    with open(f"{rundir}/parsl.log", "r") as logfile:
        re1 = re.compile('.* Parsl task (.*) try (.*) launched on executor (.*) with executor id (.*)')
        re2 = re.compile('.* Task ([0-9]+) submitted to WorkQueue with id ([0-9]+).*')
        re3 = re.compile('([^ ]+ [^\.]+)(?:\.([0123456789]+))? parsl.bps.* GRAPH_EVALUATE_COMMAND_LINE ([0-9]+) (.+)')
        for line in logfile:
            # the key lines i want for now from parsl.log look like this:
            # Parsl task 562 try 0 launched on executor WorkQueueExecutor with executor id 337
            m = re1.match(line)
            if m:
                # logger.info(f"Line matched p->wqe bind: {line}, {m}")
                task_id = m.group(1)
                # logger.info(f"Task ID {task_id}")
                task_context = base_context.get_context(task_id, "parsl.task")
                try_id = m.group(2)
                # logger.info(f"Try ID {try_id}")
                try_context = task_context.get_context(try_id, "parsl.try")
                executor_id_context = try_context.get_context("executor", "parsl.try.executor")
                # the point of this log file line is to alias it
                # separate importing of executor-specific log file will populate
                # the parsl.try.executor context via the below aliased context
                executor_name = m.group(3)
                executor_id = m.group(4)
                executor_context = base_context.get_context(executor_name, "parsl.executor")
                executor_context.alias_context(executor_id, executor_id_context)
            # 2021-08-16 20:47:29.796 parsl.executors.workqueue.executor:933 [DEBUG]  Task 1 submitted to WorkQueue with id 2

            m = re2.match(line)
            if m:
                # logger.info(f"Line matched wqe->wq bind: {line}, {m}")

                executor_id = m.group(1)
                wq_id = m.group(2)

                # WorkQueue is hard-coded in the parsl source for this
                # log line... but need to properly disambiguate the executor
                # name - perhaps by including the executor name in the relevant
                # log message instead of the hard-coded string WorkQueue.
                executor_context = base_context.get_context("work_queue", "parsl.executor")
                context = executor_context.get_context(executor_id, "parsl.try.executor")
                wq_context = executor_context.get_context("workqueue_task_ids", "wq.ids")
                wq_task_context = wq_context.alias_context(wq_id, context)

            # 2021-09-19 11:34:48.767 parsl.bps:601 [INFO]  GRAPH_EVALUATE_COMMAND_LINE 46913058699280 get_input_file_paths
            # or
            # 2021-09-30 03:02:49 parsl.bps:595 MainProcess(45704) MainThread [INFO]  GRAPH_EVALUATE_COMMAND_LINE 46913303024400 format
            # which is missing fractional timestamp... so that fractional part should be optional...

            m = re3.match(line)
            if m:
                # logger.info("Line matched parsl.bps GRAPH_EVALUATE_COMMAND_LINE")
                timestamp = m.group(1)
                fractime = m.group(2)
                graph_id = m.group(3)
                state = m.group(4)

                # maybe this padding is largely unneeded if its being used as a fraction?
                # can I just pull out the whole time, including fraction if its there or
                # not?
                if fractime is None or len(fractime) == 0:
                    fractime = "000000"  # 0 microseconds
                elif len(fractime) == 6:
                    pass # ok
                elif len(fractime) == 3:
                    fractime += "000"  # pad to microsecs
                else:
                    raise ValueError(f"Cannot parse fractional time {fractime} as ms or us - expecting either 0, 3 or 6 digits, from log line: {line}")
#ValueError: Cannot parse fractional time 9 as ms or us - expecting either 3 or 6 symbols, from log line: 2021-09-30 03:02:49 parsl.bps:595 MainProcess(45704) MainThread [INFO]  GRAPH_EVALUATE_COMMAND_LINE 46913303024400 format

                parsl_bps_context = base_context.get_context("parsl_bps", "parsl.bps")
                graphs_context = parsl_bps_context.get_context("graph", "parsl.bps.graphs")
                graph_context = graphs_context.get_context(graph_id, "parsl.bps.graph")

                event = Event()

                event.time = datetime.datetime.strptime(timestamp + "." + fractime, "%Y-%m-%d %H:%M:%S.%f").timestamp()

                event.type = state
                graph_context.events.append(event)
                


    logger.info("Finished importing parsl.log")


def import_work_queue_python_timing_log(base_context: Context, rundir: str):
    # These logs (like the workqueue results files) aren't scoped properly
    # by executor - if there were two work queue executors in a run they
    # would conflict.
    wq_context = base_context.get_context("work_queue", "parsl.executor")
    dirs = os.listdir(f"{rundir}/function_data/")
    cre = re.compile('^([0-9\\.]+) ([^ ]+)\n$')
    for dir in dirs:
        wqe_task_id = str(int(dir))  # normalise away any leading zeros
        wq_task_context = wq_context.get_context(wqe_task_id, "parsl.try.executor")
        epf_context = wq_task_context.get_context("epf", "parsl.wq.exec_parsl_function")
        # now import the log_file into epf_context
        filename = f"{rundir}/function_data/{dir}/log"
        if os.path.exists(filename):
            with open(filename) as f:
                for line in f:
                    # 1629049247.4333403 LOADFUNCTION
                    m = cre.match(line)
                    if m:
                        event = Event()
                        event.time = float(m.group(1))
                        event.type = m.group(2)
                        epf_context.events.append(event)


def import_work_queue_transaction_log(base_context, rundir):
    # TODO: how to determine if we should import this log? should it be
    # triggered by an entry in the parsl.log file that declares that a
    # WQ executor exists?
    # for now doing my testing, I'll assume that there will be a log in the
    # WorkQueueExecutor/ subdirectory

    executor_name = "work_queue"

    executor_context = base_context.get_context(executor_name, "parsl.executor")
    wq_context = executor_context.get_context("workqueue_task_ids", "wq.ids")

    logger.info("Importing Work Queue transaction log")
    # TODO; this WorkQueueExecutor subdir is really the name of the executor
    # so should come from somewhere (eg log?) or * be used to find all
    # transaction logs?
    with open(f"{rundir}/{executor_name}/transaction_log") as transaction_log:
        cre = re.compile('([0-9]+) [0-9]+ TASK ([0-9]+) ([^ ]+) .*')
        cre2 = re.compile('([0-9]+) [0-9]+ TASK ([0-9]+) RUNNING .* FIRST_RESOURCES ({.*})$')
        for line in transaction_log:
            m = cre.match(line)
            if m:
                # logger.info(f"Line matched: {line}, {m}")
                task_id = m.group(2)
                status = m.group(3)
                # logger.info(f"WQ task {task_id} status {status}")
                wq_task_context = wq_context.get_context(task_id, "parsl.try.executor")
                event = Event()
                event.time = float(m.group(1)) / 1000000.0
                event.type = status
                wq_task_context.events.append(event)

                # now check if this was a running message with resource annotation
                # and if so, parse out the resource json:
                # 1629846648874121 200544 TASK 1 RUNNING 10.128.9.197:58802  FIRST_RESOURCES {"memory":[2700,"MB "],"disk":[0,"MB"],"gpus":[0,"gpus"],"cores":[1,"cores"]}
                rm = cre2.match(line)
                if rm:
                    resources_json = rm.group(3)
                    # now annotate the wq_task_context with the memory usage value
                    resources = json.loads(resources_json)

                    assert resources['memory'][1] == 'MB'
                    # might encounter other units sometime, and need to normalise?
                    wq_task_context.wq_resource_memory = int(resources['memory'][0]) 


    logger.info("Done importing Work Queue transaction log")


def import_parsl_rundir(base_context: Context, rundir: str) -> None:
    logger.info(f"Importing rundir {rundir}")

    # things we might find in the rundir:

    # almost definitely parsl.log - this has lots of task timing info in it,
    # a third source of task times distinct from the two monitoring db times.
    # It also has bindings between task IDs and executor IDs, and in the
    # workqueue case, bindings between wq-executor ID and work queue IDs.
    # The task timing info might be interesting for when people aren't using
    # the monitoring db, although the broad story at the moment should probably
    # still be that if you want to analyse parsl-level task timings, use the
    # monitoring db.

    import_parsl_log(base_context, rundir)
    import_work_queue_transaction_log(base_context, rundir)
    import_work_queue_python_timing_log(base_context, rundir)

    # workqueue debug log - this is what I'm most interested in integrating
    # alongside the monitoring db as it will link parsl monitoring DB state
    # transitions with WQ level transitions.

    logger.info(f"Finished importing rundir {rundir}")


def import_workflow(base_context: Context, db: sqlite3.Connection, run_id: str, rundir_map: Tuple[str, str], parsl_tz_shift: float) -> None:
    logger.info(f"Importing workflow {run_id}")

    context = base_context.get_context(run_id, "parsl.workflow")

    cur = db.cursor()

    rundir = None

    # TODO: sql injection protection (from eg hostile user sending hostile db - run_id is not sanitised)
    for row in cur.execute(f"SELECT strftime('%s', time_began), strftime('%s',time_completed), rundir FROM workflow WHERE run_id = '{run_id}'"):
        # in a well formed DB will iterate only once

        start_event = Event()
        start_event.type = "start"
        start_event.time = float(row[0]) + parsl_tz_shift
        context.events.append(start_event)

        if row[1] is not None:
            # the workflow might not have ended / might not have been recorded
            # as ended.
            end_event = Event()
            end_event.type = "end"
            end_event.time = float(row[1]) + parsl_tz_shift
            context.events.append(end_event)

        rundir = row[2]
        # TODO: we'll get the last rundir silently discarding
        # others if there are multiple workflows with the same ID
        # rather than giving an error...

    assert rundir is not None

    # rewrite the rundir using the rundir map
    logger.info(f"Remap attempt: rundir={rundir} prefix={rundir_map[0]}")
    if rundir.startswith(rundir_map[0]):
        logger.info(f"Remapping rundir: {rundir}")
        rundir = rundir_map[1] + rundir.removeprefix(rundir_map[0])
        logger.info(f"Remapped rundir: {rundir}")
    else:
        logger.info("Not remapping rundir")

    import_workflow_tasks(context, db, run_id, parsl_tz_shift)

    # there are also things defined in the parsl log (indeed, a decent amount
    # of information could come from the parsl.log file without any
    # monitoring.db at all - and maybe that's an interesting mode to support...)

    import_parsl_rundir(context, rundir)

    # c2 = import_workflow_parsl_log(context, run_id, rundir)

    # TODO: a heirarchy merge operator that lets c2 be overlaid on top of
    # the existing context. This means that the import_workflow_parsl_log
    # importer does not need an existing monitoring.db based context graph
    # to already exist - meaning it should be more amenable to use on files
    # without the monitoring db.
    # However, it then needs a notion of identity between the trees, which
    # is not implemented at the moment: how much should that identity
    # structure be baked into the code rather than specified as part of the
    # merge? This is similar to a JOIN operation, but deeply heirarchical...
    # There's also the question of context identity: previously a python
    # Context object was a context: object identity was context identity,
    # which I intended to use for expressing DAGs, by using DAGs of objects.
    # This "merge" operator gets rid of that: two Context objects (which may
    # be referred to in complicated fashions elsewhere) now need to become
    # one Context object (either re-using one of the exising ones or
    # a third new one).
    # If we're specifying keys, we're getting a bit schema-ey. But specifying
    # join keys as part of the JOIN / merge makes sense if it looks like
    # SQL style JOINs, where the fields to join on are specified as part of
    # the JOIN, not as part of the schema.
    # A different database-like approach is rather than ever calling
    # the Context constructor directly, there is a "context.declare_or_find(key)"
    # (or more simply phrased context.subcontext(type, key))
    # call which allows either the existing keyed context or a new one if
    # it does not exist - to be accessed, and modified. In that way, the
    # Context objects remain unique to their keys. And the database consists
    # of an incrementally appended collection of contexts - an importer may
    # add subcontexts to any existing context.
    # This means there should be keys in the formal query model - either
    # on contexts or on the context/subcontext edge - I don't have a feel
    # for which is better - probably on the edge, because keys make sense
    # in a context, and subcontexts can be in many contexts. Eg a try with
    # key 0 makes sense in a context of a task key n in workflow key uuuuu,
    # but doesn't in a collection of tries from many tasks, where they might
    # instead be keyed by executor job id (or even unkeyed)

    logger.info(f"Done importing workflow {run_id}")
    return context


def import_monitoring_db(root_context: Context, dbname: str, rundir_map: Tuple[str, str], parsl_tz_shift: float) -> Context:
    """This will import an entire monitoring database as a context.
    A monitoring database root context does not contain any events
    directly; it contains each workflow run as a subcontext.
    """
    logger.info("Importing context from monitoring db")
    context = root_context.get_context("monitoring", "parsl.monitoring.db")
    context.type = "parsl.monitoring.db"
    context.name = "Parsl monitoring database " + dbname

    # TODO: can this become a with: ?
    db = sqlite3.connect(dbname,
                         detect_types=sqlite3.PARSE_DECLTYPES |
                         sqlite3.PARSE_COLNAMES)

    # create a subcontext for each workflow row

    cur = db.cursor()

    # TODO this limit 1 is to only import a single workflow
    # select * from workflow order by time_began DESC limit 1;
    # for row in cur.execute("SELECT run_id FROM workflow ORDER BY time_began DESC LIMIT 1"):
    for row in cur.execute("SELECT run_id FROM workflow"):
        run_id = row[0]
        logger.info(f"workflow: {run_id}")

        import_workflow(context, db, run_id, rundir_map = rundir_map, parsl_tz_shift=parsl_tz_shift)

    db.close()

    logger.info("Finished importing context from monitoring db")
    return context
