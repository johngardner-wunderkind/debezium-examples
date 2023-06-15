#!/usr/bin/env python
from datetime import timezone
import datetime
import time
import sys
import schedule
import mysql.connector
import random
from ksuid import Ksuid
import pprint

pp = pprint.PrettyPrinter(indent=4)

mysql_con = mysql.connector.connect(
    host="mysql",
    port="3306",
    user="root",
    password="debezium",
    database="user_history"
)

mycursor = mysql_con.cursor()

# make basic sub/unsub simulation rules
repeat_customer_size = 10
repeat_customer_probs = 0.75
bouncex_devices = 100
repeat_devices = [100 + i for i in range(repeat_customer_size)]
repeat_ksuids = [str(Ksuid()) for i in range(repeat_customer_size)]

print(f"repeat devices: {repeat_devices}")
print(f"repeat ksuids: {repeat_ksuids}")

def gen_raw_event():
    dt = datetime.datetime.now(timezone.utc)
    utc_time = dt.replace(tzinfo=timezone.utc)
    utc_timestamp: float = utc_time.timestamp()
    repeat_cust_index = random.randint(0, repeat_customer_size - 1)
    id = str(Ksuid())
    if random.random() <= repeat_customer_probs:
        id = repeat_ksuids[repeat_cust_index]
    device_id = random.randint(0, bouncex_devices)
    if random.random() <= repeat_customer_probs:
        device_id = repeat_devices[repeat_cust_index]
    name = "optin_granted"
    if random.random() > 0.5:
        name = "unsubscribe"

    sql = f"""
    INSERT INTO sub_unsub_events (id, name, created_at, received_at, md5, websiteid, 
      deviceid, source, version, og_uri, ip, user_agent)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE name = case when name = 'unsubscribe' then 'optin_granted' else 'unsubscribe' end, 
     created_at = VALUES(created_at), received_at = VALUES(received_at)
    """

    val = (
        id,
        name,
        utc_timestamp,
        utc_timestamp,
        "checksum",
        2889,
        device_id,
        "sms",
        "0.0.1",
        "http://example.com/uri/hello.world?asd=bvc",
        "1.2.3.4",
        "I am a user-agent"
    )

    try:
        print(f"Saving new sub/unsub event to MySQL table for {id} repeat customer: {id in repeat_ksuids}")
        pp.pprint(sql)
        mycursor.execute(sql, val)
        query_res = mycursor.fetchall()
        pp.pprint(query_res)
        mysql_con.commit()

    except Exception as e:
        print("Exception: %s" % str(e), file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":

    gen_raw_event()
    schedule.every(1).seconds.do(gen_raw_event)

    while True:
        schedule.run_pending()
        time.sleep(1)
