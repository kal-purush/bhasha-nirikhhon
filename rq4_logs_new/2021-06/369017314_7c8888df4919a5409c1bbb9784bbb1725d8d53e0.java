/*
 * Copyright (c) 2021 Cable Television Laboratories, Inc.
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.siddhi.extension.map.p4.trpt.sourcemapper;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.siddhi.annotation.Example;
import io.siddhi.annotation.Extension;
import io.siddhi.core.exception.MappingFailedException;
import io.siddhi.core.stream.input.source.InputEventHandler;
import io.siddhi.extension.map.json.sourcemapper.JsonSourceMapper;


/**
 * Custom mapper for P4 TRPT JSON strings by extending the base JSON mapper.
 */
@Extension(
        name = "p4-trpt-json",
        namespace = "sourceMapper",
        description = "This extension is a P4 TRPT JSON-to-Event input mapper. Transports that accept these messages" +
                " can utilize this extension,",
        examples = {
                @Example(
                        syntax = "@map(type='p4-trpt-json')",
                        description = "Best when used with kafka plugin sending Telemetry Report JSON"
                )
        }
)

public class P4TrptJsonStringSourceMapper extends JsonSourceMapper {

    private final JsonParser parser = new JsonParser();

    @Override
    protected void mapAndProcess(Object eventObject, InputEventHandler inputEventHandler)
            throws MappingFailedException, InterruptedException {

        if (eventObject instanceof String) {
            final JsonObject json = (JsonObject) parser.parse((String) eventObject);
            final JsonObject eventJson = json.getAsJsonObject("event");
            if (eventJson == null) {
                throw new MappingFailedException("Incoming JSON's root element is not 'event'");
            }
            final JsonElement telemRpt = eventJson.get("telemRpt");
            if (telemRpt == null) {
                throw new MappingFailedException("Incoming JSON's 'event' element does not contain 'telemRpt'");
            }
            final String telemRptStr = telemRpt.toString();
            String telemRptStrCleansed = telemRptStr.replace("\\\"", "\"");
            telemRptStrCleansed = telemRptStrCleansed.substring(1);
            telemRptStrCleansed = telemRptStrCleansed.substring(0, telemRptStrCleansed.length() - 1);
            super.mapAndProcess(telemRptStrCleansed, inputEventHandler);
        }
    }
}