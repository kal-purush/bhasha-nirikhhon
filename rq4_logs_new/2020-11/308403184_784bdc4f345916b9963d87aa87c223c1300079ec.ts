import { APIGatewayProxyResult } from "aws-lambda";
import { importProductsFile } from ".";
import { withCorsHeaders } from "../../../utils/withCorsHeaders";

const signedUrl = "https://bucket.eu-west-1.amazonaws.com/uploaded";

jest.mock("aws-sdk", () => ({
  S3: class S3 {
    constructor() {
      return {
        getSignedUrlPromise: () => signedUrl,
      };
    }
  },
}));

describe("#importProductsFile", () => {
  it("should return correct response with signed url", async () => {
    const response = await importProductsFile({
      queryStringParameters: { name: "test.csv" },
    });

    expect(response).toMatchObject(
      withCorsHeaders({
        statusCode: 200,
        body: signedUrl,
      })
    );
  });

  it("should return status 400 if file name is empty", async () => {
    const response = await importProductsFile({
      queryStringParameters: { name: "" },
    });

    expect(response).toMatchObject(
      withCorsHeaders({ statusCode: 400 } as APIGatewayProxyResult)
    );
  });

  it("should return status 500 if something went wrong", async () => {
    const response = await importProductsFile({});

    expect(response).toMatchObject(
      withCorsHeaders({ statusCode: 500 } as APIGatewayProxyResult)
    );
  });
});