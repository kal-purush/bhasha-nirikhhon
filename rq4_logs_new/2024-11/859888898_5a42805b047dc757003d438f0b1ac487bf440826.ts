import { RuleTester } from 'eslint';
import { describe, it } from 'vitest';
import includeDocsForEndpoints from './include-docs-for-endpoints';

const ruleTester = new RuleTester({
  parser: require.resolve('@typescript-eslint/parser'),
  parserOptions: {
    ecmaVersion: 2018,
    sourceType: 'module',
    project: './tsconfig.json',
  },
});

describe('include-docs-for-endpoints', () => {
  it('should enforce documentation URLs for endpoint properties', () => {
    ruleTester.run('include-docs-for-endpoints', includeDocsForEndpoints, {
      valid: [
        {
          code: `
            const config: ProxyConfiguration = {
              // https://developers.zoom.us/docs/api/meetings/#tag/meetings/POST/users/{userId}/meetings
              endpoint: '/users/me/meetings',
              data: zoomInput,
              retries: 10
            };
          `,
        },
        {
          code: `
            const config: ProxyConfiguration = {
              // See API docs: https://api.example.com/docs
              endpoint: '/api/v1/users',
              retries: 10
            };
          `,
        },
        {
          code: `
            const config: ProxyConfiguration = {
              /* Documentation: https://api.example.com/docs */
              endpoint: '/api/v1/users',
              retries: 10
            };
          `,
        },
      ],
      invalid: [
        {
          code: `
            const config: ProxyConfiguration = {
              endpoint: '/users/me/meetings',
              data: zoomInput,
              retries: 10
            };
          `,
          errors: [
            {
              message: 'Endpoint properties should include a documentation URL in a comment above',
            },
          ],
        },
        {
          code: `
            const config: ProxyConfiguration = {
              // Configuration for user meetings
              endpoint: '/users/me/meetings',
              retries: 10
            };
          `,
          errors: [
            {
              message: 'Endpoint properties should include a documentation URL in a comment above',
            },
          ],
        },
      ],
    });
  });
}); 