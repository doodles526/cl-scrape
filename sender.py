#!/usr/bin/python
import rethinkdb as rdb
import threading
import urllib
import httplib
import time

lock = threading.Lock()
text_data = list()

def text_data_pop():
    lock.acquire()
    data = text_data.pop()
    lock.release()
    return data

def text_data_append(val):
    lock.acquire()
    text_data.append(val)
    lock.release()

def send_text(numstring, message):
    params = urllib.urlencode({'number': numstring, 'message': message})
    headers = {"Content-type": "application/x-www-form-urlencoded", "Accept": "text/plain"}
    conn = httplib.HTTPConnection("textbelt.com:80")
    conn.request("POST", "/text", params, headers)
    response = conn.getresponse()
    return response.status, response.reason


def text_worker():
    send_times = list()
    while True:
        while len(text_data) != 0:
            while len(send_times) > 2:
                if time.time() - send_times[0] > 195:
                    del send_times[0]
            data = text_data_pop()
            msg = "\nURL: " + str(data['url']) +\
                  "\nName: " + str(data['description'])
            if 'location' in data:
                  msg = msg + "\nLocation: " + str(data['location'])
            msg = msg + "\nPrice: "
            if 'price' in data:
                msg = msg + str(data['price'])
            else:
                msg = msg + 'Not Listed'
            msg = msg + "\nFull Description:\n" + str(data['long_description'])
            send_text('5412308021', msg)
            send_times.append(time.time())

def handle_val(val):
    text_data_append(val)

def get_db_conn():
    return rdb.connect('localhost', 28015)


def main():
    txtr = threading.Thread(target=text_worker)
    txtr.start()
    while True:
        conn = get_db_conn()
        curs = rdb.db('cl_scrape').table('classified_scrape').filter(
                lambda entity: entity.has_fields('long_description')).changes(squash=False).run(conn)

        for change in curs:
            print change
            if change['new_val']:
                handle_val(change['new_val'])

if __name__ == '__main__':
    main()
