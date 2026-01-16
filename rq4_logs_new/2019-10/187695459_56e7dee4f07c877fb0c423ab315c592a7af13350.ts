import GitHubConfig from '../../config/github';
import GitHubRepository from '../../repositories/factories/github';
import GithubController from '../github';

class GithubControllerFactory {
  static make(): GithubController {
    return new GithubController(
      GitHubConfig.get(),
      GitHubRepository.make(),
    );
  }
}

export default GithubControllerFactory;