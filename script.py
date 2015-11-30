#!/usr/bin/env python
import process

links = process.all_event_links()

failed_links = []
success_links = []

for link in links:
    print ''
    try:
        fail_flag = False
        rounds_info = process.all_rounds_info(link)
        print 'Round info parsed for event {}'.format(rounds_info[0][2])
        for round_ in rounds_info:
            try:
                process.process_results_link(*round_)
                print '>>>>>>{} Round {} Successfully Processed<<<<<<'.format(round_[1], round_[2])
            except:
                print 'XXXXXX{} Round {} Failed XXXXXXX'.format(round_[1], round_[2])
                fail_flag = True
        print ''
        if fail_flag:
            failed_links.append(link)
        else:
            success_links.append(link)
            print 'Event {} Successfully Parsed!'.format(rounds_info[0][2]) 
    except:
        failed_links.append(link)

print ''
print 'Failed Links:'
for link in failed_links:
    print link

