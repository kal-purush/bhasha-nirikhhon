export default (parts: string[], enforceTrailingSlash: boolean) => {
	let startingSlash = parts.length && parts[0].startsWith('/');
	const url = parts.reduce(
		(allSegments, segment) => {
			if (segment) {
				let cleaned = segment.trim().replace(/(^\/|\/$)/g, '');
				if (cleaned) {
					if (/^(http:)|(https:)\/\//.test(cleaned)) {
						startingSlash = false;
						return [cleaned];
					}
					allSegments.push(cleaned);
				}
			}
			return allSegments;
		},
		[] as string[],
	)

	const trailingSlash = enforceTrailingSlash || parts.length && parts[parts.length - 1].trim().endsWith('/')
	return `${startingSlash ? '/' : ''}${url.join('/')}${trailingSlash ? '/' : ''}`
}