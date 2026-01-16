export type Interval = {
	durationSeconds: number;
	intensityPercent: number;
};

type CourseData = {
	timeMinutes: number;
	intensityPercent: number;
};

const calculateWithStartEndTimes = (intervals: Interval[]) => {
	const withStartEndTimes = intervals.map(i => ({ ...i, startTimeMinutes: 0, endTimeMinutes: 0 }));
	for (let i = 0; i < intervals.length; ++i) {
		withStartEndTimes[i].startTimeMinutes = i === 0 ? 0 : withStartEndTimes[i - 1].endTimeMinutes;
		withStartEndTimes[i].endTimeMinutes =
			withStartEndTimes[i].startTimeMinutes + withStartEndTimes[i].durationSeconds / 60;
	}
	return withStartEndTimes;
};

const convertToCourseData = (intervals: Interval[]): CourseData[] => {
	const withStartAndEndTimes = calculateWithStartEndTimes(intervals);

	return withStartAndEndTimes.flatMap(t => [
		{ timeMinutes: t.startTimeMinutes, intensityPercent: t.intensityPercent },
		{ timeMinutes: t.endTimeMinutes, intensityPercent: t.intensityPercent }
	]);
};

export const buildMRCFileString = (name: string, description: string, intervals: Interval[]) => {
	const courseHeader = `
		[COURSE HEADER]\n
		VERSION = 2\n
		UNITS = ENGLISH\n
		DESCRIPTION = ${description}\n
		FILE NAME = ${name}\n
		MINUTES PERCENT\n
		[END COURSE HEADER]\n
		`;

	const courseData = convertToCourseData(intervals);
	const courseDataString = courseData
		.map(d => `${d.timeMinutes.toFixed(2)}\t${d.intensityPercent.toFixed(0)}\n`)
		.join('');

	return `
	${courseHeader}
	[COURSE DATA]\n
	${courseDataString}
	[END COURSE DATA]
	`;
};