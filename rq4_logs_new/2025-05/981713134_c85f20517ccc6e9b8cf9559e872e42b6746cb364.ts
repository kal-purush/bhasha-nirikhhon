export enum AllowedParameters {
	topics = 'topics',
	groupId = 'groupId',
	timeout = 'timeout',
	acks = 'acks',
}

export function parseDataFromArgs(key: AllowedParameters): string[] {
	const args = process.argv
	const keyIndex = args.findIndex(arg => arg === `--${key}`)

	if (keyIndex !== -1 && keyIndex + 1 < args.length) {
		const argValue = args[keyIndex + 1]
		return argValue.split(',').map(value => value.trim())
	}
	return []
}