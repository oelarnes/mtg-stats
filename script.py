#!/usr/bin/env python
from update_events import update_events
from scrape_results import get_new_results

update_events()
get_new_results(40)
