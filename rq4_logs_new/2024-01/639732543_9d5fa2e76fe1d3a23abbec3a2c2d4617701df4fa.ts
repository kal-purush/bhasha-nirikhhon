import awsServerlessExpress from "@vendia/serverless-express"
import { promiseResult, success } from "TiFBackendUtils"
import { addBenchmarking, addRoutes, createApp } from "../app.js"
import { addCognitoTokenVerification } from "../auth.js"
import { ServerEnvironment } from "../env.js"
import { addEventToRequest } from "../index.js"

const testEnv: ServerEnvironment = {
  environment: "dev",
  eventStartWindowInHours: 1,
  maxArrivals: 4,
  setProfileCreatedAttribute: () => promiseResult(success())
}

export const app = createApp()
addBenchmarking(app)
addCognitoTokenVerification(app, testEnv)
addRoutes(app, testEnv)

// for stage tests
addEventToRequest(app)
export const handler = awsServerlessExpress({ app })