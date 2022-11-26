#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from mrjob.job import MRJob
import mrjob
import pdb

class MRCount(MRJob):

    def mapper(self, _, line):
        # identifying the columns and by which they're seperated 
        (ID, session_id, Type, window_id, browser, device_type, os, host, current_url, referrer, referring_domain, pathname,
         path1, path2, path3, path4, path5, number_of_pages, event_type, date, year, month, day, month_name, day_name, 
         week_label, time, day_parts, browser_version, screen_height, screen_width, viewport_height, viewport_width, 
         min_time, max_time, duration_hours, duration_minutes, duration_seconds, total_pages, invest) = line.split('\t')
        # identify the column we're interested in to take it as a key
        yield duration_minutes, 1


    def combiner(self, duration_minutes, count):
        yield duration_minutes, sum(count)

    def reducer(self, duration_minutes, count):
        yield duration_minutes, sum(count)

if __name__ == '__main__':
    MRCount.run()

