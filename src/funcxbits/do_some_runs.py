
import funcx
from funcx.utils.errors import TaskPending
import logging

logger = logging.getLogger("dnpc.funcx.demoapp")


target_endpoint = '4b116d3c-1703-4f8f-9f6f-39921e5864df'

def sleep(duration=30):
    import time
    time.sleep(duration)

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, filename="do_some_runs.log", format="%(created)f %(asctime)s %(name)s:%(lineno)d %(processName)s(%(process)d) %(threadName)s [%(levelname)s]  %(message)s")
    logger.info("APP CREATE_FUNCX_CLIENT")
    c = funcx.FuncXClient()

    logger.info("APP REGISTER_FUNCTION")
    sleep_uuid = c.register_function(sleep)
    logger.info(f"APP REGISTER_FUNCTION_POST {sleep_uuid}")

    result_refs = []
    logger.info("APP LAUNCH_STAGE")
    for n in range(0,500):
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
                print(r)
                logger.info(f"TASK {n} POLL_END_COMPLETE")
        logger.info(f"POLL_LOOP_END {loop_count}")

    logger.info("APP END_MAIN")
