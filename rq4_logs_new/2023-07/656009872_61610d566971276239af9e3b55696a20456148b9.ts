import { Commit, FileTreeQuery, GitActor, Tree } from '@lib/generated/graphql';
import { parseLineItems } from './parse-file-explorer';

export const parseFileTree = (data?: FileTreeQuery) => {
  const latestCommit = data?.repository?.ref?.target as Commit;
  const commitHistory = (latestCommit.history.nodes ?? [])[0];
  const author = commitHistory?.author as GitActor | undefined;

  const fileTree = data?.repository?.tree as Tree | undefined;
  const files = parseLineItems(fileTree);

  return {
    latestCommitSummary: {
      shortCommitHash: commitHistory?.abbreviatedOid,
      lastCommitMessage: commitHistory?.messageHeadline,
      lastCommittedDate: commitHistory?.committedDate,
      commitAuthor: author?.user?.login,
      commitAuthorAvatarUrl: author?.avatarUrl,
    },
    files,
  };
};