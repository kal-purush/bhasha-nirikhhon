/*
 * Copyright © 2019 Atomist, Inc.
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

import * as k8s from "@kubernetes/client-node";
import { ServiceRegistration } from "@atomist/sdm";

export function elasticsearch(tag: string = "latest", password: string = "MagicWord"): ServiceRegistration<k8s.V1Container> {

    const spec: k8s.V1Container = {
        name: "elasticsearch",
        image: `docker.elastic.co/elasticsearch/elasticsearch:${tag}`,
        imagePullPolicy: "IfNotPresent",
        resources: {
            limits: {
                cpu: 0.5,
                memory: "512Mi",
            },
            requests: {
                cpu: 0.5,
                memory: "512Mi",
            },
        },
        env: [{
            name: "discovery.type",
            value: "single-node",
        }, {
            name: "xpack.monitoring.enabled",
            value: "false",
        }, {
            name: "xpack.ml.enabled",
            value: "false",
        }, {
            name: "ELASTIC_PASSWORD",
            value: password,
        }, {
            name: "ES_JAVA_OPTS",
            value: "-Xms256m -Xmx256m",
        },
        ],
    } as any;

    return {
        name: "elasticsearch",
        service: async goalEvent => {
            if (goalEvent.repo.name === "pochta") {
                return {
                    type: "kubernetes",
                    spec,
                };
            }
            return undefined;
        },
    };
}