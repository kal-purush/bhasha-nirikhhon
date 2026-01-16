import { SearchRequest } from "@/models/search/SearchRequest";
import EntityService from "@/services/EntityService";
import { IM } from "@/vocabulary/IM";
import axios from "axios";

describe("EntityService.ts", () => {
  beforeEach(() => {
    jest.resetAllMocks();
    axios.get = jest.fn().mockResolvedValue("axios get return");
    axios.post = jest.fn().mockResolvedValue("axios post return");
  });

  it("can get partial entity", async() => {
    const result = await EntityService.getPartialEntity("testIri", ["pred_1", "pred_2", "pred_3"]);
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity/partial", { params: { iri: "testIri", predicate: "pred_1,pred_2,pred_3" } });
    expect(result).toBe("axios get return");
  });

  it("can post advancedSearch", async() => {
    const request = new SearchRequest;
    const cancelToken = axios.CancelToken.source().token;
    const result = await EntityService.advancedSearch(request, cancelToken);
    const api = process.env.VUE_APP_API;
    expect(axios.post).toHaveBeenCalledTimes(1);
    expect(axios.post).toHaveBeenCalledWith(api + "api/entity/search", request, { cancelToken: cancelToken });
    expect(result).toBe("axios post return");
  });

  it("can get entity", async() => {
    const result = await EntityService.getEntity("testIri");
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity", { params: { iri: "testIri" } });
    expect(result).toBe("axios get return");
  });

  it("can get entity definition dto", async() => {
    const result = await EntityService.getEntityDefinitionDto("testIri");
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity/definition", { params: { iri: "testIri" } });
    expect(result).toBe("axios get return");
  });

  it("can get entity im lang", async() => {
    const result = await EntityService.getEntityImLang("testIri");
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity", { params: { iri: "testIri" }, headers: { accept: "application/imlang"}, responseType: "text" });
    expect(result).toBe("axios get return");
  });

  it("can get entity parents", async() => {
    const result = await EntityService.getEntityParents("testIri");
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity/parents", { params: { iri: "testIri" } });
    expect(result).toBe("axios get return");
  });

  it("can get entity children", async() => {
    const cancelToken = axios.CancelToken.source().token;
    const result = await EntityService.getEntityChildren("testIri", cancelToken);
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity/children", { params: { iri: "testIri" }, cancelToken: cancelToken });
    expect(result).toBe("axios get return");
  });

  it("can get entity usages", async() => {
    const result = await EntityService.getEntityUsages("testIri", 1, 25);
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity/usages", { params: { iri: "testIri", page: 1, size: 25 } });
    expect(result).toBe("axios get return");
  });

  it("can get usages total records", async() => {
    const result = await EntityService.getUsagesTotalRecords("testIri");
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity/usagesTotalRecords", { params: { iri: "testIri" } });
    expect(result).toBe("axios get return");
  });

  it("can get entity members", async() => {
    const result = await EntityService.getEntityMembers("testIri", true, true, 1000);
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity/members", { params: { iri: "testIri", expandMembers: true, expandSubsets: true, limit: 1000 } });
    expect(result).toBe("axios get return");
  });

  it("can get scheme options", async() => {
    EntityService.getEntityChildren = jest.fn().mockReturnValue("axios get return");
    const result = await EntityService.getSchemeOptions();
    const api = process.env.VUE_APP_API;
    expect(EntityService.getEntityChildren).toHaveBeenCalledTimes(1);
    expect(EntityService.getEntityChildren).toHaveBeenCalledWith(IM.CODE_SCHEME);
    expect(result).toBe("axios get return");
  });

  it("can save entity", async() => {
    const result = await EntityService.saveEntity({ iri: "testIri", name: "testName" });
    const api = process.env.VUE_APP_API;
    expect(axios.post).toHaveBeenCalledTimes(1);
    expect(axios.post).toHaveBeenCalledWith(api + "api/entity", { iri: "testIri", name: "testName" });
    expect(result).toBe("axios post return");
  });

  it("can get entity graph", async() => {
    const result = await EntityService.getEntityGraph("testIri");
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity/graph", { params: { iri: "testIri" } });
    expect(result).toBe("axios get return");
  });

  it("can get entity term codes", async() => {
    const result = await EntityService.getEntityTermCodes("testIri");
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity/termCode", { params: { iri: "testIri" } });
    expect(result).toBe("axios get return");
  });

  it("can get semantic properties", async() => {
    const result = await EntityService.getSemanticProperties("testIri");
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity/semanticProperties", { params: { iri: "testIri" } });
    expect(result).toBe("axios get return");
  });

  it("can get data model properties", async() => {
    const result = await EntityService.getDataModelProperties("testIri");
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity/dataModelProperties", { params: { iri: "testIri" } });
    expect(result).toBe("axios get return");
  });

  it("can get complex mappings", async() => {
    const result = await EntityService.getComplexMappings("testIri");
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity/complexMappings", { params: { iri: "testIri" } });
    expect(result).toBe("axios get return");
  });

  it("can get entity summary", async() => {
    const result = await EntityService.getEntitySummary("testIri");
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity/summary", { params: { iri: "testIri" } });
    expect(result).toBe("axios get return");
  });

  it("can get get namespaces", async() => {
    const result = await EntityService.getNamespaces();
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity/namespaces");
    expect(result).toBe("axios get return");
  });

  it("can get complex members", async() => {
    const result = await EntityService.getComplexMembers("testIri");
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity/complexMembers", { params: { iri: "testIri" } });
    expect(result).toBe("axios get return");
  });

  it("can get axioms", async() => {
    const result = await EntityService.getAxioms("testIri");
    const api = process.env.VUE_APP_API;
    expect(axios.get).toHaveBeenCalledTimes(1);
    expect(axios.get).toHaveBeenCalledWith(api + "api/entity/axioms", { params: { iri: "testIri" } });
    expect(result).toBe("axios get return");
  });
});