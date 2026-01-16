export function getTime(dateStr: string) {
	return new Date(dateStr).toISOString().substring(11, 16);
}