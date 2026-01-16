export const groupedQueryLogOrderByEnum = [
	'title',
	'maxDuration',
	'minDuration',
	'averageDuration',
	'minSize',
	'maxSize',
	'averageSize',
	'totalSize',
	'count',
	'first',
	'last',
	'totalTime'
] as const;

export type GroupedQueryLogOrderByEnumType = (typeof groupedQueryLogOrderByEnum)[number];

type GroupedQueryLogOrderByEnumTitles = {
	[K in GroupedQueryLogOrderByEnumType]: string;
};

// This will be valid for demonstration purposes
const groupedQueryLogEnumTitles: GroupedQueryLogOrderByEnumTitles = {
	title: 'Title',
	maxDuration: 'Max Duration',
	minDuration: 'Min Duration',
	averageDuration: 'Average Duration',
	minSize: 'Min Size',
	maxSize: 'Max Size',
	averageSize: 'Average Size',
	totalSize: 'Total Size',
	count: 'Count',
	first: 'First',
	last: 'Last',
	totalTime: 'Total Time'
};

export const groupedQueryLogOrderByEnumToText = (input: GroupedQueryLogOrderByEnumType) => {
	return groupedQueryLogEnumTitles[input];
};