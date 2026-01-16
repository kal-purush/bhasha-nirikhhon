import UprnService from "@/services/UprnService";
import axios from "axios";

describe("UprnService.ts", () => {
  const api = process.env.VUE_APP_UPRN_API;
  const username = process.env.VUE_APP_UPRN_USERNAME || "";
  const password = process.env.VUE_APP_UPRN_PASSWORD || "";
  const userId = process.env.VUE_APP_UPRN_USERID || "";

  beforeEach(() => {
    jest.resetAllMocks();
    axios.get = jest.fn().mockResolvedValue("axios get return");
    axios.post = jest.fn().mockResolvedValue("axios post return");
  });

  it("can find uprn", async() => {
    const result = await UprnService.findUprn("test address", "test area");
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "/getinfo", { params: { adrec: "test address", qpost: "test area" }, auth: { username: username, password: password } });
    expect(result).toBe("axios get return");
  });

  it("can get uprn", async() => {
    const result = await UprnService.getUprn(123456);
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "/getuprn", { params: { uprn: 123456 }, auth: { username: username, password: password } });
    expect(result).toBe("axios get return");
  });

  it("can get activity", async() => {
    const result = await UprnService.getActivity();
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "/activity", { params: { u: userId }, auth: { username: username, password: password } });
    expect(result).toBe("axios get return");
  });

  it("can download", async() => {
    const result = await UprnService.download("test filename");
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "/filedownload2", { params: { userid: userId, filename: "test filename" }, responseType: "blob", auth: { username: username, password: password } });
    expect(result).toBe("axios get return");
  });

  it("can upload", async() => {
    const formData = new FormData();
    const blob = new Blob(["test data"], );
    formData.append("file", blob, undefined);
    formData.append("userid", userId);
    const result = await UprnService.upload(blob);
    expect(axios.post).toHaveBeenCalledTimes(1);
    expect(axios.post).toHaveBeenCalledWith(api + "/fileUpload2", formData, { headers: { "Content-Type": "multipart/form-data" }, auth: { username: username, password: password } });
    expect(result).toBe("axios post return");
  });
});