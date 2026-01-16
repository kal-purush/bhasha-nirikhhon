import { CliCommand } from '@carnesen/cli';
import { GithubRepo } from '../github-repo';

export const cloneCarnesenCliCommand = CliCommand({
	name: 'clone-carnesens',
	description: 'Clone @carnesen repositories',
	async action() {
		const repos = await GithubRepo.carnesenRemotes();
		for (const repo of repos) {
			repo.clone();
		}
	},
});