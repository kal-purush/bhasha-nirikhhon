"use client";
import { useEffect, useState } from "react";

/**
 * Custom React Hook to track the status of a media query.
 *
 * @param  query - The media query string to match against.
 * @returns  - The current status of the media query (true if it matches, false otherwise).
 *
 * @example
 * // Example usage:
 * const isMobile = useMediaQuery('(max-width: 768px)');
 * console.log('Is mobile?', isMobile);
 */
export function useMediaQuery(query: string): boolean {
	const [value, setValue] = useState(false);

	/**
	 * Handles the change event for the media query.
	 *
	 * @param  event - The change event object.
	 */
	function onChange(event: MediaQueryListEvent): void {
		setValue(event.matches);
	}

	useEffect(() => {
		// Create a MediaQueryList object based on the provided query string.
		const result = window.matchMedia(query);

		// Attach the onChange handler to listen for changes in the media query status.
		result.addEventListener("change", onChange);

		// Set the initial value based on the current state of the media query.
		setValue(result.matches);

		// Cleanup: Remove the event listener when the component is unmounted or the query changes.
		return () => {
			result.removeEventListener("change", onChange);
		};
	}, [query]);

	return value;
}