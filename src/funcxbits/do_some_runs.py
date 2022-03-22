import concurrent.futures
import funcx
import funcx.sdk.executor
from funcx.utils.errors import TaskPending
import logging

logger = logging.getLogger("dnpc.funcx.demoapp")

# production tutorial endpoint
# target_endpoint = '4b116d3c-1703-4f8f-9f6f-39921e5864df'
# funcx_service=None

#funcx_service="https://api.dev.funcx.org/v2"
#target_endpoint="2238617a-8756-4030-a8ab-44ffb1446092"
#wsuri = "wss://api.dev.funcx.org/ws/v2/"
# other mess from test conftest.py that might be useful:
#            "results_ws_uri": "wss://api.dev.funcx.org/ws/v2/",

# benc dev cluster:
funcx_service="http://amber.cqx.ltd.uk/v2"
target_endpoint="5eefe259-5846-4ee6-8ccb-e395a6185ceb"
wsuri = "ws://amber.cqx.ltd.uk/ws/v2/"


NUM_ITERS = 200

def sleep(duration=10):
    import time
    start=time.time()
    import platform # after start time - so this looks a bit ugly
    workerid = platform.node()
    time.sleep(duration)
    end=time.time()
    return (start, end, workerid)

if __name__ == "__main__":

    uuid_to_taskn_map = {}
    futures = []

    logging.basicConfig(level=logging.DEBUG, filename="do_some_runs.log", format="%(created)f %(asctime)s %(name)s:%(lineno)d %(processName)s(%(process)d) %(threadName)s [%(levelname)s]  %(message)s")
    logger.info("APP CREATE_FUNCX_CLIENT")
    c = funcx.FuncXClient(funcx_service_address=funcx_service, results_ws_uri=wsuri)
    executor = funcx.sdk.executor.FuncXExecutor(c)

    #logger.info("APP REGISTER_FUNCTION")
    # sleep_uuid = c.register_function(sleep)
    #logger.info(f"APP REGISTER_FUNCTION_POST {sleep_uuid}")

    result_refs = []
    logger.info("APP LAUNCH_STAGE")
    for n in range(0,NUM_ITERS):
      logger.info(f"TASK {n} SUBMIT")
      # result_uuid = c.run(endpoint_id = target_endpoint, function_id=sleep_uuid)
      future = executor.submit(sleep, endpoint_id= target_endpoint)
      result_uuid = future.batch_id
      logger.info(f"TASK {n} SUBMIT_POST {result_uuid}")
      futures.append(future)
      # result_refs.append((n, result_uuid))
      uuid_to_taskn_map[result_uuid] = n

    logger.info("APP LAUNCH_STAGE_POST")


    logger.info("APP POLLING_STAGE")

    for f in concurrent.futures.as_completed(futures):
      r = f.result()
      print(r)
      print(f.batch_id)
      n = uuid_to_taskn_map[f.batch_id]
      print(n)
      print("===")
      logger.info(f"TASK {n} POLL_END_COMPLETE")
      logger.info(f"TASK_INNER_TIME {n} {r[0]} {r[1]} {r[2]}")

    logger.info("APP END_MAIN")
