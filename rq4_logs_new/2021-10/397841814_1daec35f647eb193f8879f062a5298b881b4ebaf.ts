import { catalogBatchProcess } from "../handler";
import { formatJSONResponse } from "../../../libs/apiGateway";

beforeEach(() => {
  jest.clearAllMocks();
});

const mockResult = {
  products: {
    title: "Cocktail-3",
    description: "Cocktail-3 description",
    price: "3",
    count: "30",
  },
  eventSource: "aws:sqs",
  awsRegion: "eu-west-1",
};
const mockEvent = {
  Records: [
    {
      body: JSON.stringify(mockResult),
    },
  ],
};

describe("catalogBatchProcess handler", () => {
  test("should return status code 200", async () => {
    const actual = await catalogBatchProcess(mockEvent);
    expect(actual.statusCode).toEqual(200);
  });

  test("should return correct responce", async () => {
    const actual = await catalogBatchProcess(mockEvent);
    const mockResponse = mockEvent.Records.map((record) => {
      return JSON.parse(record["body"]);
    });
    const expected = formatJSONResponse(mockResponse);

    expect(actual).toMatchObject(expected);
  });
});