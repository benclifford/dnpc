import logging

from parsl.log_utils import set_stream_logger

from dnpc.structures import Context

from dnpc.importer_general import import_monitoring_db

from dnpc.plots import \
    (plot_parsl_workflows_cumul,
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


# __name__ is not package qualified in __main__
logger = logging.getLogger("dnpc.main")


def stats_total_in_bps_input_file_path(monitoring_db_context: Context):

    accum_time = 0.0  # seconds

    # TODO: assumes only one workflow
    wf_context = monitoring_db_context.subcontexts_by_type("parsl.workflow")[0]
    if len(wf_context.subcontexts_by_type("parsl.bps")) < 1:
        logger.info(f"no BPS context for workflow {wf_context} - skipping")
        return

    parsl_bps_context = wf_context.subcontexts_by_type("parsl.bps")[0]
    graphs_context = \
        parsl_bps_context.subcontexts_by_type("parsl.bps.graphs")[0]
    graph_context = graphs_context.subcontexts_by_type("parsl.bps.graph")[0]


    # copy, rather than alias, the events list
    next_events = [e for e in graph_context.events]

    next_events.sort(key=lambda e: e.time)

    while next_events:
        here_event = next_events.pop(0)
        if here_event.type == "get_input_file_paths":
            next_event = next_events.pop(0)
            # this will fail if that call never returned,
            #  eg due to forced run end
            t = next_event.time - here_event.time
            accum_time += t

    logger.info(f"Accumulated time in bps get_input_file_paths: {accum_time}s")


def main() -> None:
    set_stream_logger(name="dnpc", level=logging.INFO)
    logger.info("dnpc start")

    # is a root context really a "root"? I can, I think, later on, embed it
    # inside another context - so this is more like an empty context
    # constructor?
    # in which case, why not Context() without a helper? I think I was assuming
    # I could declare "roots" but thats not how things have evolved?
    root_context = Context.new_root_context()

    # This is to deal with log files being moved around to different paths,
    # for exmaple if moved into an archive area or to a different system.
    # The parsl monitoring DB at least contains hard-coded paths - this
    # map will swap the original prefix for the new prefix.
    # BUG: this needs to be without the / suffix if it points to the whole
    # rundir. that would mess up situations where the run-id has gone up
    # beyond 999 # and so has prefixes that overlap. eg 012 is a prefix of
    #  0123, while 012/ is not a prefix of 0123/

    runinfo = "/home/benc/parsl/src/dnpc/sample-data/cori1/"
    parsl_rundir_map = \
        ("/global/cscratch1/sd/bxc/run202108/gen3_workflow/runinfo/",
         runinfo)

    import_monitoring_db(root_context,
                         runinfo + "/monitoring.db",
                         rundir_map=parsl_rundir_map,
                         parsl_tz_shift=7.0 * 3600.0)

    monitoring_db_context = root_context.get_context("monitoring",
                                                     "parsl.monitoring.db")

    logger.info(f"got monitoring db context {monitoring_db_context}")

    # Now make a subset context -- that is, a monitoring db context that
    # contains only some selected subspans.  # Most immediately, because I
    # only want to plot one workflow, but more generally I could select a
    # subset # of workflows and so doing subsets this way rather than picking
    # out an individual workflow is maybe more general.

    logger.info("Replacing monitoring db context with a setset of it")

    # what might a helper method look like? select-by with a lambda?

    monitoring_db_context = monitoring_db_context.select_subcontexts(
                              lambda key, ctx:
                              key == "849c455b-e807-45ae-a188-04ab1c906e86"
                            )

    # this is a map-style select ^
    # there might also be a fold style "select the latest..." ?

    plot_parsl_workflows_cumul(monitoring_db_context)
    plot_tasks_summary_cumul(monitoring_db_context)
    plot_tasks_status_cumul(monitoring_db_context)
    plot_tries_cumul(monitoring_db_context)
    plot_tries_runtime_histo_submit(monitoring_db_context)
    plot_tries_runtime_histo_wq(monitoring_db_context)
    plot_wq_task_runtime_histo(monitoring_db_context)
    plot_wq_running_to_parsl_running_histo(monitoring_db_context)
    plot_wq_parsl_worker_cumul(monitoring_db_context)
    plot_all_task_events_cumul(monitoring_db_context)
    plot_tasks_status_streamgraph(monitoring_db_context)
    plot_tasks_status_streamgraph_submit(monitoring_db_context)
    plot_tasks_status_streamgraph_wq(monitoring_db_context)
    plot_task_running_event_stacked_and_streamgraph_wq(monitoring_db_context)
    plot_execute_function_to_parsl_running_histo(monitoring_db_context)
    plot_tasks_launched_streamgraph_wq_by_type(monitoring_db_context)
    plot_tasks_running_streamgraph_wq_by_type(monitoring_db_context)
    plot_tasks_running_streamgraph_wq_by_type_mem_weighted(
      monitoring_db_context)
    stats_total_in_bps_input_file_path(monitoring_db_context)
    logger.info("dnpc end")


if __name__ == "__main__":
    main()
