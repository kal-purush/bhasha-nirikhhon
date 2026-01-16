from google.cloud import pubsub_v1
from random_word import RandomWords
from quote import quote
import time
import json
import sys, getopt

TOPIC_ID = "quote_topic"

def main(argv):
    argumentList = sys.argv[1:]
    options = "hp:"
    long_options = ["help", "project_id="]
    try:
        arguments, values = getopt.getopt(argumentList, options, long_options)
    except getopt.error:
        print('publisher.py --project_id <project-id>')
        sys.exit(2)

    project_id = ''
    for currentArgument, currentValue in arguments:
        if currentArgument in ("-h", "--Help"):
            print('publisher.py --project_id <project-id>')
            sys.exit(2)

        elif currentArgument in ("-p", "--project_id"):
            project_id = currentValue
            print ("Project ID:", project_id)

    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, TOPIC_ID)

    while True:
        r = RandomWords()
        w = r.get_random_word()
        qte = quote(w, limit=1)

        if qte is not None:
            data_str = json.dumps(qte[0])
            data = data_str.encode("utf-8")
            publisher.publish(topic_path, data)
            print(f"Published messages to {topic_path}: {data_str}")
            time.sleep(5)

if __name__ == "__main__":
    main(sys.argv[1:])