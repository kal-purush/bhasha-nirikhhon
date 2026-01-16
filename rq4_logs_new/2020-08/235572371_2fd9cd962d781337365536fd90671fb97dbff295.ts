/*
 * Copyright © 2020 Atomist, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { CommandHandler, github, repository, secret } from "@atomist/skill";
import { DeleteBranchConfiguration } from "../configuration";
import { listStaleBranchesOnRepo } from "../listStaleBranches";

export const handler: CommandHandler<DeleteBranchConfiguration> = async ctx => {
	const params = await ctx.parameters.prompt<{
		owner: string;
		name: string;
		branch: string;
		apiUrl: string;
		defaultBranch: string;
		msgId: string;
		cfg: string;
		channels: string;
	}>({
		owner: {},
		name: {},
		branch: {},
		msgId: {},
		cfg: {},
		defaultBranch: {},
		apiUrl: {},
		channels: {},
	});
	const cfg = ctx.configuration.find(c => c.name === params.cfg);

	const credential = await ctx.credential.resolve(
		secret.gitHubAppToken({
			owner: params.owner,
			repo: params.name,
		}),
	);
	const api = github.api(
		repository.gitHub({
			owner: params.owner,
			repo: params.name,
			credential,
		}),
	);

	try {
		await api.git.deleteRef({
			owner: params.owner,
			repo: params.name,
			ref: `heads/${params.branch}`,
		});
	} catch (e) {
		// ignore
	}

	return listStaleBranchesOnRepo(
		cfg,
		ctx,
		{ ...params, channels: JSON.parse(params.channels) },
		params.msgId,
	);
};