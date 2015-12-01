#!/usr/bin/env python
import process

links = process.all_event_links()

failed_links = []

for link in links:
  failed_links.extend(process.process_event_link(link))

print 
print '=====SCRAPING COMPLETE====='
print 
print 'Failed Links:'
for link in failed_links:
    print link

