import GitHub, { Tag } from '../util/GitHub';
import * as semver from 'semver';
import { existsSync } from 'fs';
import { join } from 'path';

export interface TagFilter {
	(tag: Tag, index: number, array: Tag[]): boolean;
}

/**
 * creates a path to HTML API docs
 */
export function getHtmlApiPath(base: string, project: string, version: string) {
	return join(base, `${ project }/${ version }`);
}

/**
 * creates a path to JSON API docs
 */
export function getJsonApiPath(base: string, project: string, version: string) {
	return join(base, `${ project }-${ version }.json`);
}

/**
 * @param project project name
 * @param directory the base directory where html api docs are stored
 * @return a filter for existing html api docs
 */
export function createHtmlApiMissingFilter(project: string, directory: string): TagFilter {
	return (tag: Tag) => {
		return !existsSync(getHtmlApiPath(directory, project, tag.name));
	};
}

/**
 * @param project project name
 * @param directory the base directory where json api docs are stored
 * @return a filter for existing json api docs
 */
export function createJsonApiMissingFilter(project: string, directory: string): TagFilter {
	return (tag: Tag) => {
		return !existsSync(getJsonApiPath(directory, project, tag.name));
	};
}

/**
 * A filters only the latest
 * @param index the index of the tag
 * @return if the tag is the latest
 */
export function latestFilter(_tag: Tag, index: number, array: Tag[]) {
	return index === array.length - 1;
}

/**
 * @param comp a semver comparison
 * @return a filter to check if the tag satisfies the semver
 */
export function createVersionFilter(comp: string): TagFilter {
	return (tag: Tag) => {
		const version = semver.clean(tag.name);
		return semver.satisfies(version, comp);
	};
}

/**
 * Get a list of GitHub tags that pass the supplied filters
 * @param repo the GitHub repository
 * @param filters Tag filters to apply to the tag
 * @return a list of tags
 */
export default async function getTags(repo: GitHub, filters: TagFilter[] = []): Promise<Tag[]> {
	return (await repo.fetchTags())
		.filter(function (tag) {
			return semver.clean(tag.name);
		})
		.sort(function (a: Tag, b: Tag) {
			const left = semver.clean(a.name);
			const right = semver.clean(b.name);
			return semver.compare(left, right, true);
		})
		.filter(function (tag: Tag, index: number, array: Tag[]) {
			for (const filter of filters) {
				if (!filter(tag, index, array)) {
					return false;
				}
			}
			return true;
		});
}