import sys
import logging
import datetime
import time
import os

import config as  config
from azure.eventhub import EventHubClient, Sender, EventData

logger = logging.getLogger("azure")

ADDRESS = config.eventHubPolicy['Url']
USER = config.eventHubPolicy['User']
KEY = config.eventHubPolicy['Key']

try:
    if not ADDRESS:
        raise ValueError("No EventHubs URL supplied.")

    # Create Event Hubs client
    client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)
    sender = client.add_sender(partition="0")
    client.run()
    try:
        start_time = time.time()
        for i in range(10):
            print("Sending message: {}".format(i))
            sender.send(EventData(str(i)))
    except:
        raise
    finally:
        end_time = time.time()
        client.stop()
        run_time = end_time - start_time
        logger.info("Runtime: {} seconds".format(run_time))

except KeyboardInterrupt:
    pass