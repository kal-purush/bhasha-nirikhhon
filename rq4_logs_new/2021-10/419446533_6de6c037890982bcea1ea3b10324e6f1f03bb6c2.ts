import axios from 'axios';
import { sign } from 'jsonwebtoken';

import prismaClient from '../prisma';

type Request = {
  code: string;
}

type GithubOAuthLoginResponse = {
  access_token: string;
  type: string;
}

type GithubApiUserResponse = {
  id: number;
  login: string;
  name: string;
  avatar_url: string;
}

export class AuthenticateUserWithGithubService {
  async execute(request: Request) {
    const { code } = request;

    const url = 'https://github.com/login/oauth/access_token';

    const { data } = await axios.post<GithubOAuthLoginResponse>(url, null, {
      params: {
        client_id: process.env.GITHUB_OAUTH_CLIENT_ID,
        client_secret: process.env.GITHUB_OAUTH_CLIENT_SECRET,
        code,
      },
      headers: {
        "Accept": "application/json"
      },
    });

    const { access_token } = data;

    const response = await axios.get<GithubApiUserResponse>('https://api.github.com/user', {
      headers: {
        Authorization: `Bearer ${access_token}`,
      },
    });

    const { id: github_id, login, name, avatar_url } = response.data;

    let user = await prismaClient.user.findFirst({ where: { github_id } });

    if (!user) {
      user = await  prismaClient.user.create({
        data: {
          login,
          name,
          github_id,
          avatar_url,
        }
      });
    }

    const token = sign(
      {
        id: user.id,
        name: user.name,
        avatar_url: user.avatar_url,
      },
      process.env.JWT_SECRET,
      {
        subject: user.id,
        expiresIn: '1d',
      }
    );

    return { token, user };
  }
}