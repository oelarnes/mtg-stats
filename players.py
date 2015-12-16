import requests
import re

from bs4 import BeautifulSoup
from distance import levenshtein

from mtgdb import Cursor

def fix_name_and_country(name, country):
  if name is None:
    return (name, country)
  part = name.rpartition('[')
  if len(part[0]):
    return (part[0][:-1], part[1]+part[2])
  else:
    return (name, country)

def normalize_raw_name(raw_name):
  raw_name = raw_name.upper()
  sleep_in_patterns = ['ZVIP', 'ZZVIP', 'ZZZVIP', 'ZZ', 'ZZZ', 'ZZSIS', 'ZZFIX', 'ZZZ_', 'ZZZZZ', 'VIP', 'VIP_', 'AAVIP', 'AAA VIP -']
  for pattern in sleep_in_patterns:
    if raw_name.startswith(pattern) and not raw_name.startswith('VIPPERMAN'):
      raw_name = raw_name.rpartition(pattern)[2]
    elif raw_name.endswith(pattern):
      raw_name = raw_name.partition(pattern)[0]
  raw_name = raw_name.strip(' ()1234567890')
  last_first = list(raw_name.partition(','))
  last_first[0] = last_first[0].partition('[')[0].rstrip(' *').strip(' *')
  last_first[2] = last_first[2].rpartition('SEE SK ')[2].strip(' *').rstrip(' *') #why?? what is this??
  normalized_name = last_first[0]
  if len(last_first[2]):
    normalized_name += ', ' + last_first[2]
  return normalized_name

def normalize_full_raw_name(full_raw_name):
  return '/'.join([normalize_raw_name(name) for name in full_raw_name.split('/')])

def max_name_list(names1, names2):
  ret_names = []
  for name in names1:
    if not any([name2.startswith(name) for name2 in names2]):
      ret_names.append(name)
  for name in names2:
    if not any([name1.startswith(name) and len(name1)>len(name) for name1 in names1]):
      ret_names.append(name)
  return ret_names

def normalized_event_names(event_id):
  cursor = Cursor()
  num_rounds = cursor.execute("select max(round_num) from results_raw_table where event_id = '{}'".format(event_id))[0][0]
  all_round_names = []
  for round_num in range(num_rounds):
    names = cursor.execute("select distinct p1_name_raw from results_raw_table where event_id = '{}' and round_num = {}".format(event_id, round_num))
    names += cursor.execute("select distinct p2_name_raw from results_raw_table where event_id = '{}' and round_num = {}".format(event_id, round_num))
    all_round_names.append(list(set([normalize_raw_name(item) for sublist in names for item in sublist if '* BYE *' not in item and 'Awarded Bye' not in item])))
  cursor.close()
  return reduce(max_name_list, all_round_names, [])
  
def populate_event_player_table(event_names, event_id):
  query = "select player_id, norm_name_1, norm_name_2, norm_name_3 from player_table where "
  or_ = False
  for name in event_names:
    if not or_:
      query += "or "
    or_ = True
    query += "norm_name_1 like '{0}%' or norm_name_2 like '{0}%' or norm_name_3 like '{0}%' ".format(name)
  cursor = Cursor()
  player_table_names = cursor.execute(query)
  found_names = []
  new_names = []
  for name in event_names:
    found = False
    for idx, row in enumerate(player_table_names):
      if name in row:
        if found:
          raise 'two matches found for name ' + name
        found_names.append({'player_id':row[0], 'normalized_name':name, 'event_id':event_id})
        found = True
    if not found:
      new_names.append(name)
  player_id = cursor.execute("select max(player_id) from player_table")[0][0]
  new_players = []
  for name in new_names:
    player_id += 1
    new_players.append({'player_id':player_id, 'norm_name_1':name, 'first_event':event_id})
    found_names.append({'player_id':player_id, 'normalized_name':name, 'event_id':event_id})
  cursor.insert('event_player_table', found_names)
  cursor.insert('player_table', new_players)
  cursor.close()

