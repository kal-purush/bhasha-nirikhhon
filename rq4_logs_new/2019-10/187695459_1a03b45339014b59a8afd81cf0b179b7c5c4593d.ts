import { Response, Request } from 'express';
import { GithubOauthApp } from '../config/github';
import GitHubRepository from '../repositories/github';

class User {
  githubConfig: GithubOauthApp;

  githubRepository: GitHubRepository;

  constructor(
    githubConfig: GithubOauthApp,
    githubRepository: GitHubRepository,
  ) {
    this.githubConfig = githubConfig;
    this.githubRepository = githubRepository;

    this.mainAction = this.mainAction.bind(this);
  }

  async mainAction(req: Request, res: Response): Promise<void> {
    const { oauth } = req.params;

    const accessData = await this.githubRepository.getAccessToken(
      oauth,
      this.githubConfig,
    );
    const user = await this.githubRepository.getUserData(
      accessData.accessToken,
    );

    res.render(
      'user',
      {
        user,
      },
    );
  }
}

export default User;