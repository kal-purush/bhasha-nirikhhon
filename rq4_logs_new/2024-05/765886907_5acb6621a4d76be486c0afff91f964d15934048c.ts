import gql from "graphql-tag";
import assert from "assert";

import { server } from './setup';
import { assertTimestamp, assertUrl } from './helpers';

import { ContributorRole } from '../src/types';

it('fetches all of the contributor roles', async () => {
  // run the query against the server and snapshot the output
  const res = await server.executeOperation(
    {
      query: gql`
        query getContributorRoles {
          contributorRoles {
            id
            displayOrder
            label
            url
            description
            created
            modified
          }
        }
      `,
    },
  );
  // Validate that Apollo returned a result
  assert(res.body.kind === 'single');
  expect(res.body.singleResult.errors).toBeUndefined();
  expect(res.body.singleResult.data).toBeDefined();
  expect(res.body.singleResult.data?.contributorRoles).toBeDefined();

  // Now validate the User that was returned
  const results = res.body.singleResult.data?.contributorRoles as [ContributorRole];
  // console.log(results);

  expect(results[0].id).toBeGreaterThan(0);
  expect(results[0].displayOrder).toBeGreaterThan(0);
  expect(results[0].label.length).toBeGreaterThan(0);
  expect(assertUrl(results[0].url)).toBe(true);
  expect(assertTimestamp(results[0].created)).toBe(true);
  expect(assertTimestamp(results[0].modified)).toBe(true);
});