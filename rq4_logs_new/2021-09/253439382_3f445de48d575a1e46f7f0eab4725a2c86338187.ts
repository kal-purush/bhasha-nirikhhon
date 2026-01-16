import { ConceptSummary } from "@/models/search/ConceptSummary";

describe("ConceptSummary.ts", () => {
  it("has the correct fields", () => {
    let summary = new ConceptSummary;
    summary = { name: "TestName", iri: "TestIri", scheme: { name: "schemeName", iri: "schemeIri" }, code: "testCode", entityType: { elements: [{ name: "typeName", iri: "typeIri" }] }, isDescendentOf: [], weighting: 8, match: "testMatch", status: { name: "statusName", iri: "statusIri" } };
    expect(summary.name).toBe("TestName");
    expect(summary.iri).toBe("TestIri");
    expect(summary.scheme).toStrictEqual({ name: "schemeName", iri: "schemeIri" });
    expect(summary.code).toBe("testCode");
    expect(summary.entityType).toStrictEqual({ elements: [{ name: "typeName", iri: "typeIri" }] });
    expect(summary.isDescendentOf).toStrictEqual([]);
    expect(summary.weighting).toBe(8);
    expect(summary.match).toBe("testMatch");
    expect(summary.status).toStrictEqual({ name: "statusName", iri: "statusIri" });
  })
});