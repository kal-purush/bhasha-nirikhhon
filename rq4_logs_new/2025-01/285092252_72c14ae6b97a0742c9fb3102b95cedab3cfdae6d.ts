/// <reference path="./.sst/platform/config.d.ts" />

export default $config({
  app(input) {
    return {
      name: "aws-aurora-postgres",
      removal: input?.stage === "production" ? "retain" : "remove",
      home: "aws",
    };
  },
  async run() {
    // NAT Gateways are required for Lambda functions
    const vpc = new sst.aws.Vpc("MyVpc", {
      nat: "ec2",
      bastion: true,
    });
    const postgres = new sst.aws.Aurora("MyDatabase", {
      engine: "postgres",
      vpc,
    });
    new sst.aws.Function("MyApp", {
      handler: "index.handler",
      url: true,
      link: [postgres],
      vpc,
    });

    return {
      host: postgres.host,
      port: postgres.port,
      username: postgres.username,
      password: postgres.password,
      database: postgres.database,
    };
  },
});