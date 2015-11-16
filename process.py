#!/usr/bin/env python
from mtgdb import cursor
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

RAW_COL_NAMES = ['row_num', 'p1_name_raw', 'p1_country', 'result_raw', 'vs', 'p2_name_raw', 'p2_country', 'round_num', 'event_id']

def get_round_from_title(elem):
    html = elem.get_attribute('outerHTML')
    return html.partition('Round ')[2].partition(' ')[0] + html.partition('ROUND ')[2].partition(' ')[0]

def parse_row(row, round_num, event_id):
    soup = BeautifulSoup(row)
    values = [item.get_text() for item in soup.find_all('td')]
    values.append(round_num)
    values.append(event_id)
    results = dict(zip(RAW_COL_NAMES, values))

    return results

def process_results_link(link, driver, cursor, event_id):
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
    round_num = int(get_round_from_title(driver.find_element_by_tag_name('title')))
    print 'Processing Results for Event {}, Round {}'.format(event_id, round_num)
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
    cursor = cursor()
    cursor.insert('results_raw', results_table)
    driver.close()
    results_msg = '{0} lines parsed for event {1}, round {2}'.format(len(rows), event_id, round_num)
    print results_msg
    # load = db.load_tsv('results_raw', results_tsv)

    return 1


driver = webdriver.Chrome()
process_results_link('http://magic.wizards.com/en/events/coverage/gpatl15/round-9-results-2015-11-14', driver, cursor, 'event1')
