import assert = require("assert");
import fs = require("fs");
import path = require("path");
import mockFs = require("mock-fs");

import ProjectionLoader from "../lib/ProjectionLoader"

describe("ProjectionLoaderTests", () => {

    beforeEach(() => {
        mockFs({
            "rootPath": {
                "sub1": {
                    "sub2": {}
                },
            }
        });
    });

    afterEach(() => {
        mockFs.restore();
    });


    it("Test loading projections", () => { 
        // Write a projection file at root
        var projection = {
            alternates: {
                "src/**/*.js": "test/**/{}.js"
            }
        };


        fs.writeFileSync("project.json", JSON.stringify(projection), "utf8");

        var projectionLoader = new ProjectionLoader();
        var projections = projectionLoader.getProjections("rootPath/sub1/sub2");

        assert.strictEqual(1, projections.length);
        assert.deepEqual({basePath: ".", alternates: [
            {
                primaryFilePattern: "src/**/*.js",
                alternateFilePattern: "test/**/{}.js"
            }
        ]}, projections[0]);
    });
 });