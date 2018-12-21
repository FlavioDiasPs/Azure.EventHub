import sys
import logging
import datetime
import time
import os
import random as rand

# import config as  config
from azure.eventhub import EventHubClient, Sender, EventData

logger = logging.getLogger("azure")

ADDRESS = 'amqps://de-eh-test.servicebus.windows.net/de-ingestion-hub'
USER = 'SenderPolicy'
KEY = 'D4JZNSFXPj0QLKSzdhef5NhmuxZKEtTVeIioAIUYfHI='

# Endpoint=sb://de-eh-test.servicebus.windows.net/;
# SharedAccessKeyName=SenderPolicy;
# SharedAccessKey=D4JZNSFXPj0QLKSzdhef5NhmuxZKEtTVeIioAIUYfHI=;
# EntityPath=de-ingestion-hub

try:
    if not ADDRESS:
        raise ValueError("No EventHubs URL supplied.")

    # Create Event Hubs client
    client = EventHubClient(ADDRESS, debug=False, username=USER, password=KEY)
    
    sender = client.add_sender(partition="0")
    client.run()
    try:
        start_time = time.time()

        message = '12\n'
        for i in range(3):
            message += f"'{str(rand.randint(0, 9000))}',"
            message += f"'{str(rand.randint(0, 9000))}',"
            message += f"'{str(rand.randint(0, 9000))}',"                     
            message += f"'{str(rand.randint(0, 9000))}'\n"
            
        print("Sending message: \n{}".format(message))        
        sender.send(EventData(message))
    except:
        raise
    finally:
        end_time = time.time()
        client.stop()
        run_time = end_time - start_time
        logger.info("Runtime: {} seconds".format(run_time))

except KeyboardInterrupt:
    pass