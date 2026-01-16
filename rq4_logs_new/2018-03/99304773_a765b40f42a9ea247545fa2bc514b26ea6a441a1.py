#! /usr/bin/env python

if __name__ =='__main__':

	file = '/home/milan/workspace/strands_ws/src/battery_scheduler/data/'+ 'real_dec16_rhc'

	reward_gained = []
	total_rewards = []
	battery = 0
	error = 0
	with open(file, 'r') as f:
		for line in f:
			if 'time' not in line:
				s = line.split(' ')
				if len(s) >= 5:
					if 'gather_reward' in s:
						reward_gained.append(float(s[5]))
					total_rewards.append(float(s[5]))
					error = error + abs(float(s[4])-float(s[5]))
					battery = battery + int(s[1])
	print file
	print 'Total Reward:',sum(total_rewards)
	print 'Gained Rewards:', sum(reward_gained)
	print 'Percentage Achieved: ', float(sum(reward_gained)/sum(total_rewards))
	print 'Model Error: ', error/48

	print 'Avg BAttery Value:' , battery/len(total_rewards)