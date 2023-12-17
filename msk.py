import pykafka
import json
from pykafka import KafkaClient
from pytz import timezone
from datetime import timezone
from datetime import timedelta
from datetime import datetime

def send_msg(topic,msg):
    client=KafkaClient(hosts='xxxxxx')
    prd = client.topics[topic].get_sync_producer()
    prd._protocol_version=1  #timestamp只有在1版本的kafka数据结构中才引入
    prd.produce(msg.encode('utf-8'),timestamp=datetime.now()+ timedelta(hours=-8))

msg="""
{
    "id":"1489994324783209493261",
    "vehicleCode":"L1dwd67777",
    "receiveTime":11111348320,
    "body":{
        "vehicleType":"081",
        "s1":0,
        "s2":0
    }
}
"""
send_msg('vehicle04',msg)
