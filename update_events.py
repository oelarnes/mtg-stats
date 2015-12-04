import requests

from bs4 import BeautifulSoup
from mtgdb import Cursor
from dateutil.parser import parse
from datetime import datetime, timedelta

MAGIC_URL = 'http://magic.wizards.com'
EVENTS_URL = MAGIC_URL + '/en/events/coverage'
EVENT_TABLE_COLUMNS = ['event_id', 'event_full_name', 'day_1_date', 'day_1_rounds', 'day_2_date', 'day_2_rounds', 'day_3_date', 'day_3_rounds', 'num_players',  
    'fmt_desc', 'fmt_type', 'fmt_primary', 'fmt_secondary', 'fmt_third', 'fmt_fourth', 'season', 'champion', 'event_type', 'host_country', 'team_event', 'event_link', 
    'results_loaded']

def clean_magic_link(url):
    if url.startswith(('http://','https://')):
        return url
    elif url.startswith('/'):
        return MAGIC_URL + url

def event_id_from_link(url):
    event_id = url.rpartition('/')[2]
    if '=' in event_id:
        event_id = event_id.rpartition('=')[2]
    if event_id in ['welcome', 'Welcome']:
        event_id = url.rpartition('/')[0].rpartition('=')[2]
    if event_id == 'results':
        event_id = url.rpartition('/')[0].rpartition('/')[2]
    if '/' in event_id:
        event_id = event_id.rpartition('/')[2]
    return event_id

def info_text_to_date(text):
    if ')' not in text:
        return None
    date_str = text.partition(')')[0].lstrip(' (')
    dash_idx = date_str.find('-')
    comma_idx = date_str.rfind(',')
    if dash_idx < 0 or comma_idx < 0 or dash_idx > comma_idx:
        return None
    try:
        return parse(date_str[:dash_idx] + date_str[comma_idx:])
    except:
        return None

def info_text_to_fmt_desc(text):
    return text.partition(')')[2].lstrip(u'-\u2014\u2013 ')

def update_event(event_info):
    if 'event_link' not in event_info:
        return None
    cursor = Cursor()
    query = "select * from event_table where event_link = '{}'".format(event_info['event_link'])
    result = cursor.execute(query)
    if len(result) == 0:
        event_info['results_loaded'] = 0
        cursor.insert('event_table', [event_info])
    cursor.close()
    return

def update_events():
    r = requests.get(EVENTS_URL)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text)
        sections = soup.find_all('div', class_='bean_block')
        print 'found {} sections'.format(len(sections))
        for section in sections:
            if not section.find('span').text.endswith('Season'):
                continue
            season = section.find('span').text.partition(' ')[0]
            print 'season {}'.format(season)
            paragraphs =  section.find('div').find_all('p')
            d = {'event_type' : 'Championship', 'season' : season}
            for paragraph in paragraphs:
                for child in paragraph.children:
                    #print child
                    #print '/\/\/\/\/\/\/\/'
                    if child.name in ['b', 'strong']:
                        if 'Grand Prix' in child.text:
                            d['event_type'] = 'Grand Prix'
                        if 'Pro Tour' in child.text:
                            d['event_type'] = 'Pro Tour'
                        if 'Masters' in child.text:
                            d['event_type'] = 'Masters'
                    elif child.name in ['i','em']:
                        if 'fmt_desc' in d:
                            d['fmt_desc'] += child.text
                    elif child.name == 'br':
                        if 'event_link' in d:
                            update_event(d)
                            d = {'event_type' : d['event_type'], 'season' : season}
                    elif child.name == 'a':
                        d['event_link'] = clean_magic_link(child['href'])
                        d['event_id'] = event_id_from_link(d['event_link'])
                        if d['event_type'] == 'Championship':
                            d['event_full_name'] = child.text
                        else:
                            d['event_full_name'] = d['event_type'] + ' ' + child.text
                    elif child.name is None:
                        if 'day_1_date' not in d:
                            d['day_1_date'] = info_text_to_date(child)
                            if d['event_type'] in ['Grand Prix', 'Pro Tour'] and d['day_1_date'] is not None:
                                d['day_2_date'] = d['day_1_date'] + timedelta(1)
                            if d['event_type'] == 'Pro Tour' and d['day_1_date'] is not None:
                                d['day_3_date'] = d['day_2_date'] + timedelta(1)
                            d['fmt_desc'] = info_text_to_fmt_desc(child)
                        elif 'fmt_desc' in d:
                            d['fmt_desc'] += child
                if 'event_link' in d:
                    update_event(d)
                    d = {'event_type' : d['event_type'], 'season' : season}
    else:
        r.raise_for_status()
    return


