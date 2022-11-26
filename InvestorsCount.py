# importing MRJob and MRStep from mrjob package 
from mrjob.job import MRJob
from mrjob.job import MRStep
# Defining a class named "InvestorsBreakdown" that inherits the capabilites of "MRJob"
class InvestorsBreakdown(MRJob):
    # Define single mapreduce phase
    def steps(self):
        return [
            # The mapper function of that step is defined at "mapper_get_investor" and reducer function is "reducer_count_investor" and those are the two pieces of information the MRStep needs to work.
            MRStep(mapper=self.mapper_get_investor,
            reducer=self.reducer_count_investor)
    ]
# Define mapper to break up lines of data on tab, extracts the rating and return key value pair of rating and 1
def mapper_get_investor(self, _, line):
  (index, session_id, Type, window_id,	browser, device_type, os, host, current_url, referrer, referring_domain, pathname, path1, path2, path3, path4, path5, number_of_pages, event_type, date, year, month, day, month_name, day_name, week_label, time, day_parts, browser_version, screen_height, screen_width, viewport_height, viewport_width, min_time, max_time, duration_hours, duration_minutes, duration_seconds, total_pages, invest) = line.split('\t')
  yield invest, 1
# Reduce key values, return rating and rating count.
# Reducer function takes in eacn indvidual unique rating one through five and a list of all the one values associated with each rating and then it yields the key which is the rating (1 through 5) and the sum of all those ones which is eventually the count of each rating type.
def reducer_count_investor(self, key, values):
    yield key, sum(values)
if __name__ == '__main__':
    InvestorsBreakdown.run()