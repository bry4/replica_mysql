import requests
import secrets
import json
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

def get_secrets():

    with open('config/env.json', 'r') as env_file:
        data = json.load(env_file)

    return data


def main():

    key = get_secrets()

    MYSQL_SETTINGS = {
      "host": key["host"],
      "port": key["port"],
      "user": key["user"],
      "passwd": key["pwd"]
      }

    print(">>>listener start streaming to:mysql_data")
    stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
                                server_id = 100,
                                blocking=True,
                                resume_stream=True,
                                only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent])
    for binlogevent in stream:
        for row in binlogevent.rows:
            print(">>> start event")
            event = {"schema": binlogevent.schema,
                    "table": binlogevent.table,
                    "type": type(binlogevent).__name__,
                    "row": row
                    }
            print(">>>event",event)


    stream.close()


if __name__ == "__main__":
    main()


