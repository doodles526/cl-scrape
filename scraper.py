#!/usr/bin/python

import httplib
import urllib2
import urllib
from bs4 import BeautifulSoup
import rethinkdb as rdb
import urlparse

def create_request(base_url, extension_url='', get_args={}):
    encoded_args = urllib.urlencode(get_args)
    full_url = urlparse.urlunsplit((
            'http',
            base_url,
            extension_url,
            encoded_args,
            ''))
    request = urllib2.Request(full_url)
	
    request.add_header('User-Agent', 
        "Mozilla/5.0 (Windows NT 6.3; rv:36.0) Gecko/20100101 Firefox/36.0")
	
    return request

def get_soup(base_url, extension_url='', get_args={}):
    req = create_request(base_url, extension_url, get_args)
    resp = urllib2.urlopen(req)
    return BeautifulSoup(resp.read())

# input should be a soup from a search page
# filter_func allows for arbitrary filtering of
# the dictionary returned and should take 1 arg of dict
def get_items_from_soup(soup, base_url, filter_func=None):
    items = soup.find_all('p', 'row')
    ret_list = list()
    for item in items:
        temp = dict()
        temp['id'] = int(item['data-pid'])
        if item.find('span', 'price'):
            temp['price'] = int(item.find('span', 'price').string[1:])
        temp['description'] = item.find('a', 'hdrlnk').string
        if item.find('span', 'pnr').find('small'):
            temp['location'] = item.find('span', 'pnr').find('small').string.strip()[1:-1]
        temp['time_posted'] = item.find('time')['datetime']
        if item.find('a')['href'][0] == '/':
            temp['url'] = urlparse.urlunsplit(('http',
                                               base_url,
                                               item.find('a')['href'],
                                               '',
                                               ''))
        else:
            temp['url'] = item.find('a')['href']
        if filter_func and filter_func(temp):
            ret_list.append(temp)
        if not filter_func:
            ret_list.append(temp)

    return ret_list

def insert_long_description(item):
    split = urlparse.urlsplit(item['url'])
    soup = get_soup(split.netloc, split.path, urlparse.parse_qs(split.query))
    item['long_description'] = soup.find(id='postingbody').text.strip()


def insert_long_description_batch(items):
    for item in items:
        insert_long_description(item)			

def get_db_conn():
    return rdb.connect('localhost', 28015)

def update_db(cl_location, item_extension, search_str):
    conn = get_db_conn()
    soup = get_soup(cl_location + ".craigslist.org", 
                    '/search/' + item_extension, 
                    {'query': search_str})
    items = get_items_from_soup(soup, cl_location + ".craigslist.org")
    rdb.db('cl_scrape').table('classified_scrape').insert(items).run(conn)
    
    non_descript = list(rdb.db('cl_scrape').table('classified_scrape').filter(lambda entry: ~entry.has_fields('long_description')).run(conn))
    insert_long_description_batch(non_descript)
    rdb.db('cl_scrape').table('classified_scrape').insert(non_descript, conflict='update').run(conn)


def main():
    update_db('portland', 'mcy', 'crf')
    update_db('corvallis', 'mcy', 'crf')
    update_db('eugene', 'mcy', 'crf')
    update_db('salem', 'mcy', 'crf')
    update_db('bend', 'mcy', 'crf')
    update_db('klamath', 'mcy', 'crf')
    update_db('medford', 'mcy', 'crf')
    update_db('oregoncoast', 'mcy', 'crf')
    update_db('humboldt', 'mcy', 'crf')

if __name__ == '__main__':
    main()
