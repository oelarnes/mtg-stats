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
  last_first = raw_name.partition(',')
  normalized_name = last_first[0].strip().rstrip() 
  if len(last_first[2]):
    normalized_name += ', ' + last_first[2].strip().rstrip()
  return normalized_name

def normalize_full_raw_name(full_raw_name):
  return '/'.join([normalize_raw_name(name) for name in full_raw_name.split('/')])

def stream_names():
  cursor = Cursor()
  names = cursor.execute("select distinct p1_name_raw from results_raw_table where p1_name_raw like '%/%'")
  cursor.close()
  for name in names:
    print normalize_full_raw_name(name[0])

def max_name_list(names1, names2):
  ret_names = []
  for name in names1:
    if not any([name2.startswith(name) for name2 in names2]):
      ret_names.append(name)
  for name in names2:
    if not any([name1.startswith(name) and len(name1)>len(name2) for name1 in names1]):
      ret_names.append(name)
  return ret_names

def normalized_event_names(event_id):
  cursor = Cursor()
  num_rounds = cursor.execute("select max(round_num) from results_raw_table where event_id = '{}'".format(event_id))[0][0]
  all_round_names = []
  for round_num in range(num_rounds):
    names = cursor.execute("select distinct p1_name_raw from results_raw_table where event_id = '{}' and round_num = {}".format(event_id, round_num))
    names += cursor.execute("select distinct p2_name_raw from results_raw_table where event_id = '{}' and round_num = {}".format(event_id, round_num))
    all_round_names.append(list(set([normalize_full_raw_name(item) for sublist in names for item in sublist]))))

  event_names = reduce(max_name_list, all_round_names, [])
  query = "select player_id, norm_name_1, norm_name_2, norm_name_3 from player_table where "
  or_ = False
  for name in event_names:
    if not or_:
      query += "or "
    query += "norm_name_1 like '{0}%' or norm_name_2 like '{0}%' or norm_name_3 like '{0}%' ".format(name)
