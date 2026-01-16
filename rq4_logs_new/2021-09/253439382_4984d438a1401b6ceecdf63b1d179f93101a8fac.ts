import ConfigService from "@/services/ConfigService";
import axios from "axios";

describe("ConfigService.ts", () => {
  beforeEach(() => {
    axios.get = jest.fn().mockResolvedValue(["test config"]);
  });

  it("can get component layout", async() => {
    const result = await ConfigService.getComponentLayout("definition");
    expect(axios.get).toHaveBeenCalledTimes(1);
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledWith(api + "api/config/componentLayout", { params: { name: "definition" }});
    expect(result).toStrictEqual(["test config"]);
  });

  it("can get filter defaults", async() => {
    const result = await ConfigService.getFilterDefaults();
    expect(axios.get).toHaveBeenCalledTimes(1);
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledWith(api + "api/config/filterDefaults");
    expect(result).toStrictEqual(["test config"]);
  });
});