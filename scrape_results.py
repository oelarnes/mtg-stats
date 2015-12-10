import requests
import utils

from mtgdb import Cursor
from bs4 import BeautifulSoup
from update_events import clean_magic_link
from players import fix_name_and_country

RAW_TABLE_NAME = 'results_raw_table'
RAW_COL_NAMES = ['table_id', 'p1_name_raw', 'p1_country', 'result_raw', 'vs', 'p2_name_raw', 'p2_country', 'round_num', 'event_id', 'elim']

def get_new_results(num_events):
    cursor = Cursor()
    query = "select event_link, event_id from event_table where results_loaded=0 limit {}".format(num_events)
    event_infos = cursor.execute(query)
    cursor.close()
    for event_info in event_infos:
        mark_event(*event_info, result=process_event_link(*event_info))

def mark_event(event_link, event_id, result):
    cursor = Cursor()
    query = "UPDATE event_table set results_loaded={} where event_id='{}' and event_link='{}'".format(result, event_id, event_link)
    cursor.execute(query)
    cursor.close()
    return

def upload_round_results(results_table, event_id, round_num):
    # results_table must all have same round_num and represent all results for that round!!
    print
    print '==========Processing Results for Event {}, Round {}=========='.format(event_id, round_num)
    cursor = Cursor()
    print 'Writing {} rows'.format(len(results_table))
    cursor.insert(RAW_TABLE_NAME, results_table)
    cursor.close()
    cursor = Cursor()
    print 'New {} row count: {}'.format(RAW_TABLE_NAME, cursor.execute('select count(1) from {}'.format(RAW_TABLE_NAME))[0][0])
    cursor.close(commit=False)

def elim_results(soup, event_id, max_round_num):
    ELIM_ERR_MSG = 'Could not interpret elimation round results for event {}'.format(event_id)
    bracket_pairs = soup.find('div', class_='top-bracket-slider').find_all('div', class_='dual-players')
    bracket_pairs = [pair for pair in bracket_pairs if len(pair.find_all('div', class_='player')) == 2]
    results_table = []
    print '{} matches found in elimination rounds'.format(len(bracket_pairs))
    for idx, pair in enumerate(bracket_pairs):
        players = list(pair.find_all('div', class_='player'))
        p1 = players[0].text.strip().lstrip('()12345678 ')
        p2 = players[1].text.strip().lstrip('()12345678 ')
        p1_part = p1.partition(',')
        p2_part = p2.partition(',')
        strong = pair.find('strong').text.strip().lstrip('()12345678 ')
        result_raw = ''
        if strong == p1 or len(p1_part[2]) > 0 and len(p2_part[2]) == 0:
            result_raw = 'Won ' + p1_part[2]
        if strong == p2 or len(p2_part[2]) > 0 and len(p2_part[2]) == 0:
            if len(result_raw) > 0:
                raise Exception(ELIM_ERR_MSG)
            result_raw = 'Lost ' + utils.str_reverse(p2_part[2])
        if len(result_raw)==0:
            raise Exception(ELIM_ERR_MSG)
        p1_name_raw = utils.standardize_name(p1_part[0])
        p2_name_raw = utils.standardize_name(p2_part[0])
        if len(bracket_pairs) == 7:
            if idx < 4:
                round_num = max_round_num + 1
            elif idx < 6:
                round_num = max_round_num + 2
            else:
                round_num = max_round_num + 3
        elif len(bracket_pairs) == 3:
            if idx < 2:
                round_num = max_round_num + 1
            else:
                round_num = max_round_num + 2
        else:
            round_num = max_round_num + 1

        row = {
            'p1_name_raw' : p1_name_raw,
            'p2_name_raw' : p2_name_raw,
            'result_raw' : result_raw,
            'round_num' : round_num,
            'event_id' : event_id,
            'elim' : 1,
            'vs' : 'vs.'
        }
        results_table.append(row)

    upload_round_results(results_table, event_id, max_round_num + 1)

def all_rounds_info(soup, event_id):
    results = [el for el in soup.find_all('p') if 'RESULTS' in el.text or 'Results' in el.text]
    assert len(results) == 1
    return [(clean_magic_link(el['href']), event_id, int(el.text)) for el in results[0].parent.find_all('a')]

def event_soup(event_link):
    r = requests.get(event_link)
    if r.status_code is 200:
        soup = BeautifulSoup(r.text)
        return soup
    else:
        r.raise_for_status()
        return

def process_event_link(event_link, event_id):
    soup = event_soup(event_link)
    print 'Deleting existing rows for event {}'.format(event_id)
    cursor = Cursor()
    cursor.execute("delete from {} where event_id='{}'".format(RAW_TABLE_NAME, event_id))
    cursor.close()
    
    failed_links = []
    try:
        rounds_info = all_rounds_info(soup, event_id)
        print 'Round info parsed for event {}'.format(rounds_info[0][1])
        for round_ in rounds_info:
            try:
                process_results_link(*round_)
                print '>>>>>>{} Round {} Successfully Processed<<<<<<'.format(round_[1], round_[2])
            except Exception as error:
                print error
                print 'XXXXXX{} Round {} Failed XXXXXXX'.format(round_[1], round_[2])
                failed_links.append(round_[0])
        elim_results(soup, event_id, max([info[2] for info in rounds_info]))
        print ''
        if len(failed_links) > 0:
            print 'Event {} Incomplete :('.format(rounds_info[0][1])
            return -1
        else:
            print 'Event {} Successfully Processed!'.format(rounds_info[0][1]) 
            return 1
    except Exception as error:
        print error
        print 'Event Link {} Failed :('.format(event_link)
        return -1

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
    values.append(0)
    results = dict(zip(RAW_COL_NAMES, values))
    name_and_country_1 = fix_name_and_country(results['p1_name_raw'], results['p1_country'])
    name_and_country_2 = fix_name_and_country(results['p2_name_raw'], results['p2_country'])
    results['p1_name_raw'] = name_and_country_1[0]
    results['p1_country'] = name_and_country_1[1]
    results['p2_name_raw'] = name_and_country_2[0]
    results['p2_country'] = name_and_country_2[1]
    return results

def process_results_link(link, event_id, round_num):
    r = requests.get(link)
    if r.status_code is 200:
        soup = BeautifulSoup(r.text)
    else:
        r.raise_for_status()
        return
    results_table = [parse_row(row, round_num, event_id) for row in soup.find('table').find_all('tr') if parse_row(row, round_num, event_id) is not None]
    assert len(results_table) > 0, 'no results for event {}, round {}'.format(event_id, round_num)
    upload_round_results(results_table, event_id, round_num)



