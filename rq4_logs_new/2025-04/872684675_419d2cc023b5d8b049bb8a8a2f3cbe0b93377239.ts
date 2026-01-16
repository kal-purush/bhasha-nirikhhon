import { Database } from "./database";
import { getDomainConfig } from "./domain";
import { SecretWithEnvFallback } from "./utils";

export const GoogleOAuth = new sst.Linkable("GoogleOAuth", {
  properties: {
    clientId: SecretWithEnvFallback("GoogleOAuthClientId"),
    clientSecret: SecretWithEnvFallback("GoogleOAuthClientSecret"),
  },
});

export const Auth = new sst.aws.Auth("Auth", {
  issuer: {
    handler: "packages/auth/src/index.default",
    link: [Database, GoogleOAuth],
  },
  domain: getDomainConfig({
    type: "sub",
    name: "auth",
    stage: $app.stage,
  }),
});

export const getAuthJwksUrl = () =>
  $interpolate`${Auth.url}/.well-known/jwks.json`;