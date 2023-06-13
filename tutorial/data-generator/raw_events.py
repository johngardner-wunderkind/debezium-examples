#!/usr/bin/env python
from datetime import timezone
import datetime
import time
import sys
import schedule
import mysql.connector
import random
from ksuid import Ksuid

mydb = mysql.connector.connect(
    host="mysql",
    port="3306",
    user="root",
    password="debezium",
    database="db"
)

mycursor = mydb.cursor()

def gen_raw_event():
    print("Saving new raw event to MySQL table")

    dt = datetime.datetime.now(timezone.utc)
    utc_time = dt.replace(tzinfo=timezone.utc)
    utc_timestamp: float = utc_time.timestamp()
    sql = """
    INSERT INTO raw_events (id, name, created_at, received_at, md5, websiteid, deviceid, source, version, og_uri, ip, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    rand_device = random.randint(0, 9)
    id = str(Ksuid())
    name = "optin_granted"
    if random.random() > 0.5:
        name = "unsubscribe"
    val = (
        id,
        name,
        utc_timestamp,
        utc_timestamp,
        "checksum",
        2889,
        rand_device,
        "sms",
        "0.0.1",
        "http://example.com/uri/hello.world?asd=bvc",
        "1.2.3.4",
        "I am a user-agent"
    )

    try:
        mycursor.execute(sql, val)
        mycursor.fetchall()

    except Exception as e:
        print("Exception: %s" % str(e), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":

    gen_raw_event()
    schedule.every(1).seconds.do(gen_raw_event)

    while True:
        schedule.run_pending()
        time.sleep(1)
