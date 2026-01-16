import awsServerlessExpress from "@vendia/serverless-express"
import { addBenchmarking, addRoutes, createApp } from "../app.js"
import { addCognitoTokenVerification } from "../auth.js"
import { ServerEnvironment } from "../env.js"
import { addEventToRequest } from "../index.js"
import { testEnv } from "./devIndex.js"

const stagingEnv: ServerEnvironment = {
  ...testEnv,
  environment: "staging"
}

const app = createApp()
addBenchmarking(app)
addCognitoTokenVerification(app, stagingEnv)
addRoutes(app, stagingEnv)
addEventToRequest(app)
export const handler = awsServerlessExpress({ app })