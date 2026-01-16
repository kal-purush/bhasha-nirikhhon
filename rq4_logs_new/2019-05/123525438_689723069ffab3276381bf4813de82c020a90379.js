import axios from 'axios';
import of from 'await-of';

const apiURL = process.env.REACT_APP_API_URL;
//const apiURL = process.env.REACT_APP_LOCAL_URL;

class GraphQLClient {
  constructor({ baseURL, headers }) {
    this.axios = axios.create({
      baseURL,
      headers,
    });
  }

  async query(query) {
    const [ resp, err ] = await of(this.axios.get('/graphql', {
      params: {
        query: `query { ${query} }`
      }
    }));
    if (err || !resp || !resp.data || !resp.data.data) {
      console.warn('Failed query | error: ', err);
      return [null, err];
    }
    return [resp.data.data, null];
  }

  async mutate(mutation) {
    const [ resp, err ] = await of(this.axios.post('/graphql', {
      query: `mutation { ${mutation} }`,
    }));
    if (err || !resp || !resp.data || !resp.data.data) {
      console.warn('Failed mutation | error: ', err);
      return [null, err];
    }
    return [resp.data.data, null];
  }
}

/**
 * GraphQL Client
 */
const client = new GraphQLClient({
  baseURL: apiURL,
  headers: {
    'Accept': 'application/json',
    'Content-Type': 'application/json',
  },
});

export default client;