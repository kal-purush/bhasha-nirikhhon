'use server';

import { cookies, headers } from 'next/headers';

function getSessionId(): string {
	// Get the list of headers and cookies
	const headerList = headers();
	const setCookieHeader = headerList.get('set-cookie');
	const cookie = cookies().get('sessionId');

	// If the sessionId is present in the cookies, return it
	if (cookie?.value) return cookie.value;

	// If not found in cookies, try to extract it from the Set-Cookie header
	if (setCookieHeader) {
		const match = setCookieHeader.match(/sessionid=([^;]+);?/i);
		if (match) return match[1];
	}

	// If sessionId is not found, return an empty string
	return '';
}

export default getSessionId;