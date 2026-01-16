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

import { CommandHandler } from "@atomist/skill";
import * as _ from "lodash";
import { DeleteBranchConfiguration } from "../configuration";
import { listStaleBranchesOnRepo } from "../listStaleBranches";
import {
	SaveSkillConfigurationMutation,
	SaveSkillConfigurationMutationVariables,
} from "../typings/types";

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
	if (!cfg) {
		return {
			code: 1,
			reason: `Skill configuration not available`,
		};
	}

	await ctx.graphql.mutate<
		SaveSkillConfigurationMutation,
		SaveSkillConfigurationMutationVariables
	>("saveSkillConfiguration.graphql", {
		name: ctx.skill.name,
		namespace: ctx.skill.namespace,
		version: ctx.skill.version,
		config: {
			enabled: true,
			name: cfg.name,
			parameters: [
				{
					singleChoice: {
						name: "deleteOn",
						value: cfg.parameters.deleteOn,
					},
				},
				{
					boolean: {
						name: "staleList",
						value: cfg.parameters.staleList,
					},
				},
				{
					int: {
						name: "staleThreshold",
						value: cfg.parameters.staleThreshold,
					},
				},
				{
					stringArray: {
						name: "staleExcludes",
						value: [
							...(cfg.parameters.staleExcludes || []),
							`${params.owner}\/${params.name}#${params.branch}`, // eslint-disable-line no-useless-escape
						],
					},
				},
				{
					repoFilter: {
						name: "repos",
						value: (cfg.parameters as any).repos,
					},
				},
			],
			resourceProviders: _.map(cfg.resourceProviders, (v, k) => ({
				name: k,
				selectedResourceProviders: v.selectedResourceProviders,
			})),
		},
	});

	return listStaleBranchesOnRepo(
		cfg,
		ctx,
		{ ...params, channels: JSON.parse(params.channels) },
		params.msgId,
	);
};