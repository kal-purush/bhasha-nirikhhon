import { describe, it, expect } from "vitest";
import {
	getDocsId,
	docsIdToUrl,
	getParticipation,
	countsToTsv,
	type ParticipationCount,
} from "./participation";

describe("docs id handling", () => {
	it("extracts docs id from valid URL", () => {
		const url = "https://docs.google.com/spreadsheets/d/abc123/edit";
		expect(getDocsId(url)).toBe("abc123");
	});

	it("converts id to URL", () => {
		expect(docsIdToUrl("abc123")).toBe("https://docs.google.com/spreadsheets/d/abc123/edit");
	});

	it("returns null invalid URL", () => {
		expect(getDocsId("not a url")).toBeNull();
		expect(getDocsId("https://example.com")).toBeNull();
	});
});

describe("participation aggregation", () => {
	it("aggregates participation counts", async () => {
		const mockUrls = ["https://docs.google.com/spreadsheets/d/test/edit#gid=0"];
		const participation = await getParticipation(mockUrls);
		expect(Array.isArray(participation)).toBe(true);
	});

	it("converts counts to TSV", () => {
		const counts: ParticipationCount[] = [
			{
				name: "Test Faculty",
				total: 3,
				counts: new Map([
					["breakfast", 1],
					["lunch", 1],
					["1:1", 1],
				]),
				candidates: ["Candidate 1", "Candidate 2"],
				events: [],
			},
		];

		const tsv = countsToTsv(counts);
		expect(tsv).toContain("Test Faculty");
		expect(tsv).toContain("Candidate 1, Candidate 2");
		expect(tsv.split("\n").length).toBeGreaterThan(1);
	});
});