import * as cdk from "aws-cdk-lib"
import { CfnApp, CfnBranch } from "aws-cdk-lib/aws-amplify"
import { BuildSpec } from "aws-cdk-lib/aws-codebuild"
import { Construct } from "constructs"

export class AmplifyStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id)

    const amplify = new CfnApp(this, "AmplifyApp", {
      name: "amplify-app",
      repository: "https://github.com/yu23ki14/simplegrants",
      environmentVariables: [],
      buildSpec: BuildSpec.fromObject({
        appRoot: "frontend",
      }).toBuildSpec(),
    })

    new CfnBranch(this, "MainBranch", {
      appId: amplify.attrAppId,
      branchName: "main",
    })
  }
}