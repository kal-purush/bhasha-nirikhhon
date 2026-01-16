import * as github from "@pulumi/github";
import {BranchDefault, BranchProtection, Repository} from "@pulumi/github";

let mainBranchName = "master";

export function newPublicRepository(repoName: string,
                                    repoDescription: string,
                                    repoTopics: Array<string>) {

    return new github.Repository(repoName, {
        name: repoName,
        description: repoDescription,
        visibility: "public",
        topics: repoTopics,
        allowSquashMerge: true,
        allowRebaseMerge: false,
        allowMergeCommit: false,
        allowAutoMerge: false,
        deleteBranchOnMerge: true
    });
}

export function createDefaultBranch(resourceName: string,
                                    repo: Repository) {

    new BranchDefault(resourceName, {
        branch: mainBranchName,
        repository: repo.name
    });
}

export function createBranchProtection(resourceName: string,
                                       repo: Repository) {

    new BranchProtection(resourceName, {
        pattern: mainBranchName,
        repositoryId: repo.name,
        allowsForcePushes: false,
        enforceAdmins: true,
        requireConversationResolution: true,
        requireSignedCommits: true,
        requiredLinearHistory: true
    })
}