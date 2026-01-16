import fetch from 'unfetch';
import { task } from 'folktale/concurrency/task';

import { GRAPHQL_API_ENDPOINT } from '@/constants';

export class Cmd {
  private async checkGraphQlResponse(response) {
    const { errors, data } = await response.json();

    if (response.ok && data) return Promise.resolve(data);
    else {
      const error = new Error(errors[0].message);
      return Promise.reject(error);
    }
  }

  private buildGraphQlPayload(gqlString: string, withAuth?: boolean) {
    const token = withAuth && sessionStorage.getItem('jwt');

    return {
      method: 'post',
      credentials: 'include',
      headers: {
        ...(token && { Authorization: `Bearer ${token}` }),
        'Content-Type': 'application/json'
      },
      body: JSON.stringify({ query: `mutation ${gqlString}` })
    };
  }

  public mutation(gqlString: string, withAuth?: boolean) {
    return task(async resolver => {
      try {
        const response = await fetch(
          GRAPHQL_API_ENDPOINT,
          this.buildGraphQlPayload(gqlString, withAuth)
        );
        resolver.resolve(await this.checkGraphQlResponse(response));
      } catch (error) {
        resolver.reject(error);
      }
    });
  }
}

export default new Cmd();