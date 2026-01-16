/// <reference path="../../tasks/interface.ts" />
/// <reference path="../../tasks/thunderstoneprocessor.ts" />
/// <reference path="../../typings/tsd.d.ts" />
import thunderstoneProcessor = require("../../tasks/thunderstoneProcessor");
describe("thundestone processor tests",(): void => {
    it("should have an selector match",(): void => {
        var htmlFiles: IHtmlFile[] = [{
            content: "<html><head><link href='test.css' rel='stylesheet' /></head><body><h1>My title</h1></body><html>",
            relativePath: "",
            selectorResults:[]
        }];
        var cssFiles: ICssFile[] = [{
            fileName: "test.css",
            relativePath: "",
            selectors: ["h1"]
        }];
        var results = thunderstoneProcessor.parseHtmlFiles(htmlFiles, cssFiles);

        expect(htmlFiles[0].selectorResults.length).toBe(1);
    });
});