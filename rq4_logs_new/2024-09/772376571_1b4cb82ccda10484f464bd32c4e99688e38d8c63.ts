import { z } from "zod"
import { runMiddleware } from "../lib/Middleware"
import { tryParseAPICall } from "./APIValidation"
import { GenericEndpointSchema } from "./TransportTypes"

const mockEndpointName = "MOCK_ENDPOINT"

const apiValidator = ({endpointSchema, mockRequest, mockResponse}: {
  endpointSchema: Omit<GenericEndpointSchema, "httpRequest">, 
  mockRequest: any, 
  mockResponse: any
}) => 
  runMiddleware(
    tryParseAPICall(
      mockEndpointName, 
      endpointSchema as GenericEndpointSchema
    ),
    async () => mockResponse
  )(mockRequest)

describe("tryParseAPICall", () => {  
  it("should throw an error if the request is invalid", async () => {
    const apiCall = apiValidator({
      endpointSchema: {
        input: {
          body: z.object({
            name: z.string(),
          })
        },
        outputs: {
          status200: z.object({
            name: z.string(),
          })
        }
      },
      mockRequest: { query: { name: "John" } },
      mockResponse: { status: 404, data: { message: "Not Found" } }
    })

    await expect(apiCall).rejects.toThrow(
      `Making an invalid request to TiF API endpoint ${mockEndpointName}`
    );
  });

  it("should throw an error if the response status is unexpected", async () => {
    const apiCall = apiValidator({
      endpointSchema: {
        input: {
          body: z.object({
            name: z.string(),
          })
        },
        outputs: {
          status200: z.object({
            name: z.string(),
          })
        }
      },
      mockRequest: { body: { name: "John" } },
      mockResponse: { status: 404, data: { message: "Not Found" } }
    })

    await expect(apiCall).rejects.toThrow(
      `TiF API responded with an unexpected status code 404 and body {"message":"Not Found"}`
    );
  });
  
  it("should throw an error if response is not valid", async () => {
    const apiCall = apiValidator({
      endpointSchema: {
        input: {
          body: z.object({
            name: z.string(),
          })
        },
        outputs: {
          status200: z.object({
            name: z.string(),
          })
        }
      },
      mockRequest: { body: { name: "John" } },
      mockResponse: { status: 200, data: { name: 123 } }
    })

    await expect(apiCall).rejects.toThrow(
      `TiF API endpoint ${mockEndpointName} responded with an invalid JSON body: {"name":123}`
    );
  });
  
  it("should throw an error if response does not match constraints", async () => {
    const apiCall = apiValidator({
      endpointSchema: {
        input: {
          body: z.object({
            name: z.string(),
          })
        },
        outputs: {
          status200: z.object({
            name: z.string(),
          })
        },
        constraints: (input: any, output: any) => { return input.body.name === output.data.name }
      },
      mockRequest: { body: { name: "John" } },
      mockResponse: { status: 200, data: { name: "Johnny" } }
    })

    await expect(apiCall).rejects.toThrow(
      `TiF API endpoint ${mockEndpointName} responded with an invalid JSON body: {"name":"Johnny"}`
    );
  });
  
  it("should return response if request and response are valid", async () => {
    const apiCall = apiValidator({
      endpointSchema: {
        input: {
          body: z.object({
            name: z.string(),
          })
        },
        outputs: {
          status200: z.object({
            name: z.string(),
          })
        },
        constraints: (input: any, output: any) => { return input.body.name === output.data.name }
      },
      mockRequest: { body: { name: "John" } },
      mockResponse: { status: 200, data: { name: "John" } }
    })

    await expect(apiCall).resolves.toStrictEqual(
      {"data": {"name": "John"}, "status": 200}
    );
  });
  
  it("should allow void inputs", async () => {
    const apiCall = apiValidator({
      endpointSchema: {
        input: {},
        outputs: {
          status200: z.object({
            name: z.string(),
          })
        }
      },
      mockRequest: undefined,
      mockResponse: { status: 200, data: { name: "John" } }
    })

    await expect(apiCall).resolves.toStrictEqual(
      {"data": {"name": "John"}, "status": 200}
    );
  });
  
  it("should allow no content responses", async () => {
    const apiCall = apiValidator({
      endpointSchema: {
        input: {
          body: z.object({
            name: z.string(),
          })
        },
        outputs: {
          status204: "no-content",
        }
      },
      mockRequest: { body: { name: "John" } },
      mockResponse: { status: 204, data: undefined }
    })

    await expect(apiCall).resolves.toStrictEqual(
      {"data": undefined, "status": 204}
    );
  });
});