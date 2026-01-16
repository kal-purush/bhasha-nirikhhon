import nock from "nock";
import quantconnect, {
  EndpointDescription,
  EndpointToInterface,
  EndpointMethod,
} from ".";
import { BASE_URL } from "./api";

type EndpointTestDescription<D extends EndpointDescription<any, any, any>> =
  D["paramsRequired"] extends true
    ? { exampleParams: D["params"]; apiMethod: EndpointMethod<D> }
    : { exampleParams?: D["params"]; apiMethod: EndpointMethod<D> };

type EndpointsTestDescription = {
  [Key in keyof EndpointToInterface]: EndpointTestDescription<
    EndpointToInterface[Key]
  >;
};

describe("Endpoints", () => {
  const mockToken = "214-98islaj";
  const mockUserId = "s231348jksaj";
  const version = "v2";
  const { authenticate, files, live, projects } = quantconnect({
    userId: mockUserId,
    token: mockToken,
    version,
  });

  const endpointTests: EndpointsTestDescription = {
    authenticate: {
      apiMethod: authenticate,
    },
    "files/create": {
      exampleParams: {
        content: "",
        name: "filename.py",
        projectId: 123123,
      },
      apiMethod: files.create,
    },
    "files/read": {
      exampleParams: {
        fileName: "Some filename",
        projectId: 125512,
      },
      apiMethod: files.read,
    },
    "files/update": {
      exampleParams: {
        newFileContents: "Some new content",
        newFileName: "Old name",
        projectId: 21515,
        fileName: "Current name",
        oldFileName: "Old name",
      },
      apiMethod: files.update,
    },
    "files/delete": {
      exampleParams: {
        projectId: 12314,
        name: "Some filename",
      },
      apiMethod: files.delete,
    },
    "live/read": {
      exampleParams: {
        projectId: 2141241,
        status: "Completed",
      },
      apiMethod: live.read,
    },
    "projects/create": {
      exampleParams: {
        language: "Py",
        name: "Some project name",
      },
      apiMethod: projects.create,
    },
    "projects/read": {
      apiMethod: projects.read,
    },
    "projects/update": {
      exampleParams: {
        description: "A new description",
        name: "New name",
        projectId: 21512,
      },
      apiMethod: projects.update,
    },
    "projects/delete": {
      exampleParams: {
        projectId: 214124,
      },
      apiMethod: projects.delete,
    },
  };

  describe("API endpoints", () => {
    const mockQC = nock(`${BASE_URL}/${version}`);
    it.each(Object.entries(endpointTests))(
      "Endpoint %s should trigger correct request to QuantConnect, and pass the correct parameters",
      async (path, { apiMethod, exampleParams }) => {
        const returnObject = { success: true };
        let queryObject: typeof exampleParams;
        mockQC
          .get(`/${path}`)
          .query((actualQueryObject: any) => {
            queryObject = actualQueryObject;
            return true;
          })
          .reply(200, returnObject);
        const result = await apiMethod(exampleParams as never);
        if (exampleParams) {
          expect(queryObject).toBeDefined();
          Object.entries(queryObject!).forEach(([key, value]) => {
            expect(value).toEqual(
              // @ts-ignore
              exampleParams[key as keyof typeof exampleParams].toString()
            );
          });
        }
        expect(result).toEqual(returnObject);
      }
    );

    it.each(Object.entries(endpointTests))(
      "Endpoint %s should handle QuantConnect success: false response",
      async (path, { apiMethod, exampleParams }) => {
        const returnObject = { success: false };
        let queryObject: typeof exampleParams;
        mockQC
          .get(`/${path}`)
          .query((actualQueryObject: any) => {
            queryObject = actualQueryObject;
            return true;
          })
          .reply(200, returnObject);
        const result = await apiMethod(exampleParams as never);
        expect(result).toEqual(returnObject);
      }
    );

    it.each(Object.entries(endpointTests))(
      "Endpoint %s should handle QuantConnect server error",
      async (path, { apiMethod, exampleParams }) => {
        let queryObject: typeof exampleParams;
        mockQC
          .get(`/${path}`)
          .query((actualQueryObject: any) => {
            queryObject = actualQueryObject;
            return true;
          })
          .reply(500);
        expect(
          async () => await apiMethod(exampleParams as never)
        ).rejects.toThrowError(
          "quantconnect-client internal error: Request failed with status code 500"
        );
      }
    );
  });

  describe("Init arguments", () => {
    it("should init without errors given token and userId", () => {
      expect(() =>
        quantconnect({ userId: mockUserId, token: mockToken })
      ).not.toThrowError();
    });
    it("should throw error on missing token/user id", () => {
      expect(() => quantconnect({ userId: "", token: mockToken })).toThrow(
        "userId and token are required parameters!"
      );
      expect(() => quantconnect({ userId: mockUserId, token: "" })).toThrow(
        "userId and token are required parameters!"
      );
    });

    it("should throw error on unsupported versions", () => {
      expect(() =>
        quantconnect({ userId: mockUserId, token: mockToken, version: "v1" })
      ).toThrow(
        "quantconnect-client internal error: Invalid version used, supported versions are: v2. Got: v1!"
      );
    });
  });
});