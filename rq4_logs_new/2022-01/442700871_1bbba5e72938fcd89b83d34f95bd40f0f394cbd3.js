import axios from 'axios';
import config from '../config/config';

class GitHubProxy {
  static axios = axios.create({
    headers: {
      Authorization: `${config.get().gitHub.authToken}`,
    },
  });
  static gitHubAPIUrl = config.get().gitHub.api.baseUrl;

  static getHandler(handle) {
    return GitHubProxy.axios
      .get(`${GitHubProxy.gitHubAPIUrl}/users/${handle}`)
      .then((response) => response.data)
      .catch((err) => err);
  }

  static getRepos(repoUrl) {
    return GitHubProxy.axios
      .get(`${repoUrl}`)
      .then((response) => response.data)
      .catch((err) => err);
  }
}

export default GitHubProxy;