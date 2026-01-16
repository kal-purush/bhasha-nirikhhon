import { useEffect, useState } from 'react';
import { githubApiInstance } from '../../constant/api';

const useGithubRepo = (user: any | undefined) => {
  const [repos, setRepos] = useState<any>('init');

  const getGithubReposPage = async (url) => {
    let next = null;
    const res = await githubApiInstance.get(url);

    if (res.headers && res.headers.link) {
      const matches = /\<([^<>]+)\>; rel\="next"/.exec(res.headers.link);
      if (matches) {
        next = matches[1];
      }
    }
    const data = res.data || null;

    return {
      next,
      data,
    };
  };

  const getGithubRepo = async () => {
    let url = `/users/${user.user.reloadUserInfo.providerUserInfo[0].screenName}/repos`;
    const array = [];
    while (url) {
      const { next, data } = await getGithubReposPage(url);
      if (data) array.push(data);
      url = next;
    }

    return array.flat();
  };

  useEffect(() => {
    (async () => {
      if (user) {
        const d = await getGithubRepo();
        setRepos(() => d);
      } else {
        setRepos(() => 'no repo');
      }
    })();
  }, [user]);

  return { repos };
};

export default useGithubRepo;