export const fileOrderByEnum = [
	'title',
	'reason',
	'originalFilename',
	'size',
	'exists',
	'linked',
	'type',
	'createdAt',
	'updatedAt'
] as const;

export type FileOrderByEnumType = (typeof fileOrderByEnum)[number];

type FileOrderByEnumTitles = {
	[K in FileOrderByEnumType]: string;
};

// This will be valid for demonstration purposes
const fileEnumTitles: FileOrderByEnumTitles = {
	title: 'Title',
	reason: 'Reason',
	originalFilename: 'Filename',
	size: 'Size',
	exists: 'Exists',
	linked: 'Linked',
	type: 'Type',
	createdAt: 'Created At',
	updatedAt: 'Updated At'
};

export const fileOrderByEnumToText = (input: FileOrderByEnumType) => {
	return fileEnumTitles[input];
};