/*
 * Copyright © 2018 Atomist, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { InMemoryProject } from "@atomist/automation-client";
import assert = require("power-assert");
import {
    dockerBaseFingerprint,
} from "../../lib/fingerprints/docker";

const updateMeDockerfile = `
FROM sforzando-dockerv2-local.jfrog.io/java-atomist:0.11.1-20181115141152

MAINTAINER Jim Clark <jim@atomist.com>

RUN mkdir -p /usr/src/app \
    && mkdir -p /usr/src/app/bin \
    && mkdir -p /usr/src/app/lib

WORKDIR /usr/src/app

COPY target/lib /usr/src/app/lib

COPY target/metajar/incoming-webhooks.jar /usr/src/app/

CMD ["-Djava.net.preferIPv4Stack=true", "-jar", "/usr/src/app/incoming-webhooks.jar", "-Dclojure.core.async.pool-size=20"]

EXPOSE 8080

`;

const expectedResult = {
    name: "docker-base-image-sforzando-dockerv2-local.jfrog.io/java-atomist",
    abbreviation: "dbi-sforzando-dockerv2-local.jfrog.io/java-atomist",
    version: "0.0.1",
    data: { image: "sforzando-dockerv2-local.jfrog.io/java-atomist", version: "0.11.1-20181115141152"},
    sha: "8c81bef863e2ea2bde5d5e0567f8abba3c325eecd21135559b945c05dbf91da2",
};

describe("dockerBaseFingerprint", () => {
    describe("extract valid fingerprint", () => {
        it("should extract valid fingerprint", async () => {
            const p = InMemoryProject.from({
                repo: "foo",
                sha: "26e18ee3e30c0df0f0f2ff0bc42a4bd08a7024b9",
                branch: "master",
                owner: "foo",
                url: "https://fake.com/foo/foo.git",
            }, ({ path: "docker/Dockerfile", content: updateMeDockerfile })) as any;

            const result = await dockerBaseFingerprint(p);
            assert.deepEqual(result, expectedResult);
        });
    });

    describe("empty dockerfile, invalid fingerprint", async () => {
        it("should return undefined", async () => {
            const p = InMemoryProject.from({
                repo: "foo",
                sha: "26e18ee3e30c0df0f0f2ff0bc42a4bd08a7024b9",
                branch: "master",
                owner: "foo",
                url: "https://fake.com/foo/foo.git",
            }, ({ path: "Dockerfile", content: "" })) as any;

            const result = await dockerBaseFingerprint(p);
            assert.strictEqual(result, undefined);
        });
    });
});

// describe("applyDockerBaseFingerprint", async () => {
//     it("should successfully update the base image", async () => {
//         const p = InMemoryProject.from({
//             repo: "foo",
//             sha: "26e18ee3e30c0df0f0f2ff0bc42a4bd08a7024b9",
//             branch: "master",
//             owner: "foo",
//             url: "https://fake.com/foo/foo.git",
//         }, ({ path: "Dockerfile", content: updateMeDockerfile })) as any;

//         const result = await applyDockerBaseFingerprint(p, expectedResult);
//         assert.strictEqual(result, true);
//     });

//     it("should have updated the dockerfile content", async () => {
//         const p = InMemoryProject.from({
//             repo: "foo",
//             sha: "26e18ee3e30c0df0f0f2ff0bc42a4bd08a7024b9",
//             branch: "master",
//             owner: "foo",
//             url: "https://fake.com/foo/foo.git",
//         }, ({ path: "Dockerfile", content: updateMeDockerfile })) as any;
//         const t = (p as InMemoryProject);

//         await applyDockerBaseFingerprint(p, expectedResult);
//         const updatedDockerFileHandle = await t.getFile("Dockerfile");
//         const updatedDockerfile = await updatedDockerFileHandle.getContent();

//         assert.strictEqual(updatedDockerfile, dummyDockerFile);
//     });
// });