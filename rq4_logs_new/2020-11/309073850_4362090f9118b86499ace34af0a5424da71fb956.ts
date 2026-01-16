import { APIGatewayProxyEvent } from "aws-lambda";
import * as AWSMock from "aws-sdk-mock";
import { invoke as importProductsFile } from "../importProductsFile";

describe("importProductsFile", () => {
  test("should return 400 error if name was not provided in queryStringParameters", async () => {
    const eventMock = {
      queryStringParameters: { }
    } as APIGatewayProxyEvent;

    const result = await importProductsFile(eventMock, null, null);

    if (result) {
      expect(result.statusCode).toEqual(400);
    }
  });

  test("should return 500 error if getSignedUrl throws some error", async () => {
    const eventMock = {
      queryStringParameters: {
        name: "product.csv"
      }
    } as any as APIGatewayProxyEvent;

    AWSMock.mock("S3", "getSignedUrl", () => {
      throw "Error";
    });

    const result = await importProductsFile(eventMock, null, null);

    if (result) {
      expect(result.statusCode).toEqual(500);
    }

    AWSMock.restore("S3");
  });

  test("should return correct signed url", async () => {
    const eventMock = { queryStringParameters: { name: "product.csv" } } as any;
    const signedUrlMock = "signed-url-mock";

    AWSMock.mock("S3", "getSignedUrl", (_, __, callback) => {
      callback(null, signedUrlMock);
    });

    const result = await importProductsFile(eventMock, null, null);

    if (result) {
      expect(result.statusCode).toEqual(200);
      expect(result.body).toEqual(JSON.stringify(signedUrlMock ));
    }

    AWSMock.restore("S3");
  });
});