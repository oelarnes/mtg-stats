#!/usr/bin/env python
import process

links = process.all_event_links()

failed_links = []

for link in links[:3]:
  failed_links.extend(process.process_event_link(link))

print ''
print 'Failed Links:'
for link in failed_links:
    print link

