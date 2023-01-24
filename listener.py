import requests
import secrets
import json
from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
    DeleteRowsEvent,
    UpdateRowsEvent,
    WriteRowsEvent,
)

def main():

    MYSQL_SETTINGS = {
      "host": "",
      "port": 3306,
      "user": "",
      "passwd": ""
      }

    print(">>>listener start streaming to:mysql_data")
    stream = BinLogStreamReader(connection_settings=MYSQL_SETTINGS,
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


