from itertools import islice

def merge_meeting_times(times):
	if len(times) < 2:
		return times
	times = sorted(times)  # order by start time
	merged_times = []
	expanding_interval = list(times[0])
	for start_time, end_time in islice(times, 1, None):
		if start_time > expanding_interval[1]:  # end of an interval
			merged_times.append(tuple(expanding_interval))
			expanding_interval = [start_time, end_time]
		else:  # expand
			expanding_interval[1] = max(expanding_interval[1], end_time)
	merged_times.append(expanding_interval)
	return merged_times

if __name__ == "__main__":
	meeting_times = list(map(int, input().split(' ')))
	meeting_times = [tuple(meeting) for meeting in zip(*2*[iter(meeting_times)])]
	print(merge_meeting_times(meeting_times))