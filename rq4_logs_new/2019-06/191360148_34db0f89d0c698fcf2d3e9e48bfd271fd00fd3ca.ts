import * as request from 'supertest';

import server from '../src/server';

let app;

interface Body {
  data?: any;
  errors?: Error[];
}

class GraphQLError extends Error {
  public errors: any[];
}

async function graphqlRequest({ variables, query }): Promise<Body> {
  const { body } = await request(app)
    .post('/')
    .send({
      query,
      variables
    });

  if (body.errors) {
    const err = new GraphQLError();
    err.errors = body.errors;
    throw err;
  }
  expect(body).toHaveProperty('data');

  return body;
}

beforeAll(async () => {
  app = await server.start({ port: 4010 });
});

afterAll(() => app.close());

describe('Hello query', () => {
  it('returns hello message with name given', async () => {
    const query = `
      query {
        hello(name: "Macharia")
      }
    `;
    const variables = {};
    const { data } = await graphqlRequest({ query, variables });

    expect(data.hello).toBe('Hello Macharia');
  });
});