import define from '../../../define';
import { webhookQueue } from '../../../../../queue';

export const meta = {
	tags: ['admin'],

	requireCredential: true as const,
	requireModerator: true,

	params: {
	}
};

export default define(meta, async (ps) => {
	const jobs = await webhookQueue.getJobs(['delayed']);

	const res = [] as [string, number][];

	for (const job of jobs) {
		if (res.find(x => x[0] === job.data.userId)) {
			res.find(x => x[0] === job.data.userId)![1]++;
		} else {
			res.push([job.data.userId, 1]);
		}
	}

	res.sort((a, b) => b[1] - a[1]);

	return res;
});