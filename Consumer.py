import os
import sys
import logging
import time

import config
from azure.eventhub import EventHubClient, Receiver, Offset

logger = logging.getLogger("azure")

ADDRESS = config.eventHubPolicy['Url']
USER = config.eventHubPolicy['User']
KEY = config.eventHubPolicy['Key']

# The hell is it?
CONSUMER_GROUP = "$default"
OFFSET = Offset("-1")
PARTITION = "0"

total = 0
last_sn = -1
last_offset = "-1"

client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)
try:
    receiver = client.add_receiver(CONSUMER_GROUP, PARTITION, prefetch=5, offset=OFFSET)
    client.run()
    start_time = time.time()
    for event_data in receiver.receive(timeout=100):
        last_offset = event_data.offset
        last_message = event_data.message
        last_sn = event_data.sequence_number
        print("Received: {}, {}, {}".format(last_message, last_offset, last_sn))
        total += 1

    end_time = time.time()
    client.stop()
    run_time = end_time - start_time
    print("Received {} messages in {} seconds".format(total, run_time))

except KeyboardInterrupt:
    pass
finally:
    client.stop()