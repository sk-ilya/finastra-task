from json import JSONDecodeError
import websocket
import json
import uuid
import datetime
import pandas as pd
from record_schema import record_schema
import configparser
import time

config = configparser.ConfigParser()
config.optionxform = str
config.read('app.properties')

filtered_records_to_collect = int(config["STREAM_PROCESSOR"]["filtered_records_to_collect"])
result_path = config["STREAM_PROCESSOR"]["result_path"]
save_strategy = config["STREAM_PROCESSOR"]["save_trigger_strategy"]
server_address = config["WEBSOCKET_CONNECTION"]["server_address"]
retries_max = int(config["WEBSOCKET_CONNECTION"]["retries_on_failure"])

calculatedDataPerCountry = {}
counted_elements = 0
retries_current = 0
data_collected_and_dumped = False


def message_filter(message):
    return message["data"]["organization"] == "bank" and message["data"]["credit_score"] == 1


def check_schema(message):
    return record_schema.validate(message)


def dump_to_parquet():
    partition_columns = ["country", "year", "month", "day", "hour"]
    print("Saving collected data")

    now = datetime.datetime.now()
    rows = []
    for country in calculatedDataPerCountry.keys():
        result_for_country = calculatedDataPerCountry[country]["result_field"]
        rows.append([country, result_for_country, str(uuid.uuid4()), now, now.year, now.month, now.day, now.hour])

    df = pd.DataFrame(rows, columns=["country", "result_field", "uuid", "timestamp", "year", "month", "day", "hour"])
    df.to_parquet(result_path + str(uuid.uuid4()),
                  partition_cols=partition_columns,
                  engine='fastparquet',
                  compression='UNCOMPRESSED')
    global data_collected_and_dumped
    data_collected_and_dumped = True
    print(df.head())


def post_element_saved(ws):
    print("Collected {current} elements of {total}".format(current=counted_elements, total=filtered_records_to_collect))
    if counted_elements >= filtered_records_to_collect and not data_collected_and_dumped:
        ws.close()
        dump_to_parquet()
        print("Done!")
        exit(0)


def process_message(ws, message):
    if message_filter(message):
        data = message["data"]
        country = data["country"]
        credit_score = data["credit_score"]

        if country not in calculatedDataPerCountry:
            calculatedDataPerCountry[country] = {"result_field": 0}

        prev_sum = calculatedDataPerCountry[country]["result_field"]
        calculatedDataPerCountry[country]["result_field"] = prev_sum + credit_score

        global counted_elements
        counted_elements += 1

        post_element_saved(ws)


def on_message(ws, message):
    try:
        message_dict = json.loads(message)
        if not check_schema(message_dict):
            print("Wrong message schema: ", message)
        else:
            process_message(ws, message_dict)

    except JSONDecodeError as e:
        print("Error decoding message! (not a valid json)\n"
              "Message:\t{msg}\n"
              "Error:\t{err}".format(msg=message, err=e))


def on_error(ws, error):
    if data_collected_and_dumped:
        exit(0)

    global retries_current
    print(error)
    retries_current += 1
    if retries_current <= retries_max or retries_max == -1:
        print("Networking error, retrying... {now}/{max}".format(now=retries_current, max=retries_max))
        time.sleep(10)
        start_stream_listener()

    if retries_current > retries_max and save_strategy == "always":
        dump_to_parquet()

    print("Failed to establish connection :(. Exiting...")
    exit(1)


def on_close(ws):
    if save_strategy == "always" and not data_collected_and_dumped:
        dump_to_parquet()
    print("Socket was closed")


def on_open(ws):
    global retries_current
    retries_current = 0
    print("Successful connection")


def start_stream_listener():
    websocket.enableTrace(True)
    ws = websocket.WebSocketApp(server_address,
                                on_message=on_message,
                                on_error=on_error,
                                on_close=on_close,
                                on_open=on_open)
    ws.run_forever()


if __name__ == "__main__":
    print("Started")
    start_stream_listener()
