import rimraf from "rimraf";
import { scri } from "scriptastic";
import { ProcessHelper as ph } from "./src/processHelper";

scri.Task("clean:dist")
    .Does(() => {
        rimraf.sync("dist/**/*");
    });

scri.Task("clean:nyc")
    .Does(() => {
        rimraf.sync(".nyc_output");
    });

scri.Task("clean:coverage")
    .Does(() => {
        rimraf.sync("coverage");
    });

scri.Task("clean")
    .DependsOn("clean:dist")
    .DependsOn("clean:nyc")
    .DependsOn("clean:coverage");

scri.Task("lint")
    .Does(() => {
        ph.executeSync("tslint --project .");
    })
    .OnError(() => undefined);

scri.Task("build")
    .DependsOn("clean:dist")
    .Does(() => {
        ph.executeSync("tsc --project .");
    });

scri.Task("test")
    .DependsOn("clean:nyc")
    .DependsOn("clean:coverage")
    .Does(() => {
        ph.executeSync("nyc --all mocha", {
            env: {
                TS_NODE_PROJECT: "test/tsconfig.json",
            },
        });
    })
    .OnError(() => undefined);

scri.Task("just-pack")
    .Does(() => {
        ph.executeSync("npm pack");
    });

scri.Task("pack")
    .DependsOn("lint")
    .DependsOn("build")
    .DependsOn("test")
    .Runs("just-pack");