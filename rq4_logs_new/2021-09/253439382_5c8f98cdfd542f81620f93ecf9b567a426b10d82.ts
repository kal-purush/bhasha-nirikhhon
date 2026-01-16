import SuggestionService from "@/services/SuggestionService";
import axios from "axios";

describe("SuggestionService.ts", () => {
  beforeEach(() => {
    axios.get = jest.fn().mockResolvedValue({ data: [{ iri: "testIri", name: "testName" }]});
  });

  it("can get iri suggestions", async() => {
    const result = await SuggestionService.getIriSuggestions("testKeyword", "testWord");
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(result).toStrictEqual([{ iri: "testIri", name: "testName" }]);
  });
});