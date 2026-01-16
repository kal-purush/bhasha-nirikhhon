import { getDomainConfig } from "./domain";

export const Api = new sst.aws.Function("Api", {
  handler: "packages/server/api/src/index.default",
  url: true,
  // link: [Database, AI],
});

// we aren't really using the router much, this just enables the domain for now
// it's worth considering putting auth under this as well, but for now separate subdomain is good enough
export const ApiRouter = new sst.aws.Router("ApiRouter", {
  domain: getDomainConfig({
    type: "sub-domain",
    name: "api",
    stage: $app.stage,
  }),
  routes: { "/*": Api.url },
});