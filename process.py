from mtgdb import Cursor
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from bs4 import BeautifulSoup

RAW_TABLE_NAME = 'results_raw'
RAW_COL_NAMES = ['row_num', 'p1_name_raw', 'p1_country', 'result_raw', 'vs', 'p2_name_raw', 'p2_country', 'round_num', 'event_id']

def get_round_from_title(elem):
    html = elem.get_attribute('outerHTML')
    return html.partition('Round ')[2].partition(' ')[0] + html.partition('ROUND ')[2].partition(' ')[0]

def parse_row(row, round_num, event_id):
    soup = BeautifulSoup(row)
    values = [item.get_text() for item in soup.find_all('td')]
    while len(values) < len(RAW_COL_NAMES) - 2:
        values.append(None)
    values.append(round_num)
    values.append(event_id)
    results = dict(zip(RAW_COL_NAMES, values))
    return results

def process_results_link(link, driver, event_id):
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
    round_num = int(get_round_from_title(driver.find_element_by_tag_name('title')))
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


