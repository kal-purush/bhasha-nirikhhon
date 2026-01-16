import "../index"

import {
  OpenAPIRegistry,
  OpenApiGeneratorV3,
  extendZodWithOpenApi
} from "@asteasolutions/zod-to-openapi"
import fs from "fs"
import path from "path"
import { z } from "zod"
import { APISchema, TiFAPISchema } from "../api"

extendZodWithOpenApi(z)

const registry = new OpenAPIRegistry()

Object.entries(TiFAPISchema as APISchema).forEach(
  ([, endpointSchema]) => {
    const { httpRequest: { method, endpoint }, input: { body, query, params }, outputs } = endpointSchema

    registry.registerPath({
      method: method.toLowerCase() as Lowercase<typeof method>,
      path: `${endpoint.replace(/:(\w+)/g, "{$1}")}`,
      request: {
        // TODO conditional headers
        // headers: z.object({
        //   Authorization: z.string().openapi({ example: "1212121" })
        // }),
        params,
        query,
        body: body
          // eslint-disable-next-line multiline-ternary
          ? {
            // description: "Object with user data.", TODO: Add descriptions
            content: {
              "application/json": {
                schema: body
              }
            }
          } : undefined
      },
      responses: Object.keys(outputs).reduce((acc, key) => {
        const statusCode = key.replace("status", "")

        acc[statusCode] = statusCode === "204"
          // eslint-disable-next-line multiline-ternary
          ? { description: "No Content" } : {
            description: "Object with user data.", // TODO: require description
            content: {
              "application/json": {
                schema: outputs[key]
              }
            }
          }

        return acc
      }, {}),
      "x-amazon-apigateway-integration": {
        httpMethod: "POST", // "For Lambda integrations, you must use the HTTP method of POST for the integration request" https://docs.aws.amazon.com/apigateway/latest/developerguide/set-up-lambda-proxy-integrations.html
        uri: `{API_LAMBDA_ID}:stagingTest/invocations`,
        responses: {
          default: {
            // TODO: Generate responses
            statusCode: "200"
          }
        },
        passthroughBehavior: "when_no_match",
        contentHandling: "CONVERT_TO_TEXT",
        type: "aws_proxy"
      }
    })
  }
)

const generator = new OpenApiGeneratorV3(registry.definitions)

const specs = generator.generateDocument({
  openapi: "3.0.1",
  info: {
    title: "tifRestAPI",
    description: "API used for the TiF mobile app",
    version: `${new Date()}`
  },
  servers: [{
    url: `{API_ENDPOINT}/{basePath}`,
    variables: {
      basePath: {
        default: "staging"
      }
    }
  }]
})

const filePath = path.join(__dirname, "../specs.json")
fs.writeFileSync(filePath, JSON.stringify(specs, null, 2))

console.log("Generated api specs successfully!")