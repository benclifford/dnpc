
import funcx
from funcx.utils.errors import TaskPending
import logging

logger = logging.getLogger("dnpc.funcx.demoapp")

# production tutorial endpoint
# target_endpoint = '4b116d3c-1703-4f8f-9f6f-39921e5864df'

funcx_service="https://api.dev.funcx.org/v2"
target_endpoint="2238617a-8756-4030-a8ab-44ffb1446092"
# other mess from test conftest.py that might be useful:
#            "results_ws_uri": "wss://api.dev.funcx.org/ws/v2/",


def sleep(duration=30):
    import time
    start=time.time()
    time.sleep(duration)
    end=time.time()
    return (start, end)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, filename="do_some_runs.log", format="%(created)f %(asctime)s %(name)s:%(lineno)d %(processName)s(%(process)d) %(threadName)s [%(levelname)s]  %(message)s")
    logger.info("APP CREATE_FUNCX_CLIENT")
    c = funcx.FuncXClient(funcx_service_address=funcx_service)

    logger.info("APP REGISTER_FUNCTION")
    sleep_uuid = c.register_function(sleep)
    logger.info(f"APP REGISTER_FUNCTION_POST {sleep_uuid}")

    result_refs = []
    logger.info("APP LAUNCH_STAGE")
    for n in range(0,20):
      logger.info(f"TASK {n} RUN")
      result_uuid = c.run(endpoint_id = target_endpoint, function_id=sleep_uuid)
      logger.info(f"TASK {n} RUN_POST {result_uuid}")

      result_refs.append((n, result_uuid))

    logger.info("APP LAUNCH_STAGE_POST")


    logger.info("APP POLLING_STAGE")

    loop_count = 0
    while result_refs != []:
        logger.info(f"POLL_LOOP {loop_count}")
        for (n,uuid) in result_refs:
            logger.info(f"TASK {n} POLL_START")
            try:
                r = c.get_result(uuid)
                result_refs = [(nx, ux) for (nx, ux) in result_refs if nx != n]
            except TaskPending as e:
                logger.info(f"TASK {n} POLL_END_PENDING_{e.reason}")
                pass
            else:
                logger.info(f"TASK {n} POLL_END_COMPLETE")
                logger.info(f"TASK_INNER_TIME {n} {r[0]} {r[1]}")
                print(f"Task {n} complete")
        logger.info(f"POLL_LOOP_END {loop_count}")

    logger.info("APP END_MAIN")
