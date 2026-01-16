/**
 * Default values for pagination across the application.
 * These values should be used consistently throughout the codebase.
 */

/**
 * Default page size for all list operations.
 * This value determines how many items are returned in a single page by default.
 */
export const DEFAULT_PAGE_SIZE = 25;

/**
 * Default values for page operations
 */
export const PAGE_DEFAULTS = {
	/**
	 * Default body format for page content
	 */
	BODY_FORMAT: 'view',

	/**
	 * Whether to include page labels by default
	 */
	INCLUDE_LABELS: true,

	/**
	 * Whether to include page properties by default
	 */
	INCLUDE_PROPERTIES: true,

	/**
	 * Whether to include web resources by default
	 */
	INCLUDE_WEBRESOURCES: true,

	/**
	 * Whether to include collaborators by default
	 */
	INCLUDE_COLLABORATORS: true,

	/**
	 * Whether to include version information by default
	 */
	INCLUDE_VERSION: false,
};

/**
 * Default values for space operations
 */
export const SPACE_DEFAULTS = {
	/**
	 * Whether to include description in space results
	 */
	INCLUDE_DESCRIPTION: true,

	/**
	 * Whether to include permissions in space results
	 */
	INCLUDE_PERMISSIONS: false,
};

/**
 * Apply default values to options object.
 * This utility ensures that default values are consistently applied.
 *
 * @param options Options object that may have some values undefined
 * @param defaults Default values to apply when options values are undefined
 * @returns Options object with default values applied
 *
 * @example
 * const options = applyDefaults({ limit: 10 }, { limit: DEFAULT_PAGE_SIZE, includeLabels: true });
 * // Result: { limit: 10, includeLabels: true }
 */
export function applyDefaults<T extends Record<string, any>>(
	options: Partial<T>,
	defaults: T,
): T {
	return {
		...defaults,
		...Object.fromEntries(
			Object.entries(options).filter(([_, value]) => value !== undefined),
		),
	} as T;
}