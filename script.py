#!/usr/bin/env python
from process import process_results_link
from selenium import webdriver

driver = webdriver.Chrome()
process_results_link('http://magic.wizards.com/en/events/coverage/gpatl15/round-9-results-2015-11-14', driver, 'event1')
driver.close()
