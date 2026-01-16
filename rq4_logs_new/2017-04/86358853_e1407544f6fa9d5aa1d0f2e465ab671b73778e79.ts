/*
 * Copyright © 2017 Atomist, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { Project } from "@atomist/rug/model/Core";
import {
    CommandHandler,
    Intent,
    MappedParameter,
    Parameter,
    Secrets,
    Tags,
} from "@atomist/rug/operations/Decorators";
import {
    CommandPlan,
    HandleCommand,
    HandlerContext,
    MappedParameters,
} from "@atomist/rug/operations/Handlers";
import { Match, PathExpression, TreeNode } from "@atomist/rug/tree/PathExpression";

import { wrap } from "@atomist/rugs/operations/CommonHandlers";

@CommandHandler("MirrorGithubRepoToS3", "Mirror a GitHub repo contents to S3")
@Tags("github", "aws", "s3")
@Intent("mirror github repo s3")
@Secrets("github://user_token?scopes=repo", "secret://team?path=aws/access_key", "secret://team?path=aws/secret_key")
class GithubRepoMirror implements HandleCommand {

    @MappedParameter(MappedParameters.GITHUB_REPOSITORY)
    public repo: string;

    @MappedParameter(MappedParameters.GITHUB_REPO_OWNER)
    public owner: string;

    @Parameter({ description: "S3 bucket name", pattern: "^.*$" })
    public bucket: string;

    @Parameter({ description: "AWS region", pattern: "^.*$" })
    public region: string = "us-east-1";

    @Parameter({ description: "Glob pattern to match in GitHub repo", pattern: "^.*$" })
    public glob: string = "**/**";

    @MappedParameter("atomist://correlation_id")
    public corrid: string;

    public handle(command: HandlerContext): CommandPlan {
        const result = new CommandPlan();
        result.add(wrap({
            instruction: {
                name: "MirrorRepoToS3",
                kind: "execute",
                parameters: {
                    repo: this.repo,
                    owner: this.owner,
                    bucket: this.bucket,
                    region: this.region,
                    glob: this.glob,
                },
            },
        }, `Successfully mirrored ${this.owner}/${this.repo} to ${this.bucket} in ${this.region}`, this));
        return result;
    }
}

export let searcher = new GithubRepoMirror();