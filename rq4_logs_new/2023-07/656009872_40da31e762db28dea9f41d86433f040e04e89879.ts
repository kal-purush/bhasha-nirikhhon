type Topic =
  | (
      | {
          __typename?: 'RepositoryTopic' | undefined;
          id: string;
          topic: {
            __typename?: 'Topic' | undefined;
            name: string;
          };
        }
      | null
      | undefined
    )[]
  | null
  | undefined;

export const parseRepoTopics = (topics: Topic): string[] => {
  if (!topics) {
    return [];
  }

  return topics.reduce((acc: string[], topic) => {
    if (topic?.topic) {
      acc.push(topic?.topic.name);
    }
    return acc;
  }, []);
};