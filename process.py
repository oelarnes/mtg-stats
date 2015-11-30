import requests

from mtgdb import Cursor
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

RAW_TABLE_NAME = 'results_raw'
RAW_COL_NAMES = ['table_id', 'p1_name_raw', 'p1_country', 'result_raw', 'vs', 'p2_name_raw', 'p2_country', 'round_num', 'event_id']
MAGIC_URL = 'http://magic.wizards.com'

def clean_magic_link(url):
    if url.startswith(('http://','https://')):
        return url
    elif url.startswith('/'):
        return MAGIC_URL + url

def event_info(soup):
    info = {};
    info['link'] = clean_magic_link(soup['href'])
    info['location'] = soup.text
    extra_text = soup.next_sibling

def all_event_links():
    r = requests.get(MAGIC_URL + '/en/events/coverage')
    if r.status_code is 200:
        soup = BeautifulSoup(r.text)
        return [clean_magic_link(item['href']) for item in soup.find_all('a', class_='more') if item['href'] is not None]
    else:
        r.raise_for_status()

def all_rounds_info(event_link):
    event_id = event_link.rpartition('/')[2]
    r = requests.get(event_link)
    if r.status_code is 200:
        soup = BeautifulSoup(r.text)
        return [(clean_magic_link(el['href']), event_id, int(el.text)) for el in soup.find('p', text = 'RESULTS').parent.find_all('a')]
    else:
        r.raise_for_status()

def parse_row(soup, round_num, event_id):
    # we assume rows are either of the format RAW_COL_NAMES or 'table_id','p1_name_raw','results_raw','vs','p2_name_raw'
    values = [item.get_text() for item in soup.find_all('td')]
    if len(values) == 5:
        values.insert(2, None)
        values.insert(6, None)
    if len(values) != 7:
        return None
    values.append(round_num)
    values.append(event_id)
    results = dict(zip(RAW_COL_NAMES, values))
    return results

def process_results_link(link, event_id, round_num):
    r = requests.get(link)
    if r.status_code is 200:
        soup = BeautifulSoup(r.text)
    else:
        r.raise_for_status()
        return
    print '==========Processing Results for Event {}, Round {}=========='.format(event_id, round_num)
    results_table = [parse_row(row, round_num, event_id) for row in soup.find('table').find_all('tr') if parse_row(row, round_num, event_id) is not None]
    cursor = Cursor()
    print 'Deleting existing rows for Event {}, Round {}'.format(event_id, round_num)
    cursor.execute("delete from {} where event_id='{}' and round_num={}".format(RAW_TABLE_NAME, event_id, round_num))
    print 'Writing {} rows'.format(len(results_table))
    cursor.insert(RAW_TABLE_NAME, results_table)
    cursor.close()
    cursor = Cursor()
    print 'New {} row count: {}'.format(RAW_TABLE_NAME, cursor.execute('select count(1) from {}'.format(RAW_TABLE_NAME))[0][0])
    cursor.close(commit=False)
    return 0

def process_results_link_slow(link, driver, event_id, round_num):
    cursor = Cursor()
    driver.get(link)
    try:
        driver.find_element_by_class_name('fsrDeclineButton').click()
    except:
        pass
    try:
        warning = WebDriverWait(driver, 4).until(EC.element_to_be_clickable((By.ID, 'cookies-warning')))
        warning.find_element_by_tag_name('button').click()
    except:
        pass
    print '==========Processing Results for Event {}, Round {}=========='.format(event_id, round_num)
    rows = []
    count = 0
    while(True):
        count += 1
        table = driver.find_element_by_id('DataTables_Table_0')
        rows.extend([row.get_attribute('outerHTML') for row in table.find_elements_by_tag_name('tr')])
        next_ = driver.find_element_by_id('DataTables_Table_0_next')
        if 'disabled' in next_.get_attribute('class'):
            break
        try:
            next_.click()
        except:
            return rows, driver
    results_table = []
    for row in rows:
        results_table.append(parse_row(row, round_num, event_id))
    print 'Deleting existing rows for Event {}, Round {}'.format(event_id, round_num)
    cursor.execute("delete from {} where event_id='{}' and round_num={}".format(RAW_TABLE_NAME, event_id, round_num))
    cursor.insert(RAW_TABLE_NAME, results_table)
    cursor.close()
    cursor = Cursor()
    print 'New {} row count: {}'.format(RAW_TABLE_NAME, cursor.execute('select count(1) from {}'.format(RAW_TABLE_NAME))[0][0])
    cursor.close(commit=False)

    return 1


