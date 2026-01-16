package com.iabtechlab.openrtb.v2.json;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.iabtechlab.openrtb.v2.OpenRtbExt;

import static com.iabtechlab.openrtb.v2.json.OpenRtbJsonUtils.endArray;
import static com.iabtechlab.openrtb.v2.json.OpenRtbJsonUtils.endObject;
import static com.iabtechlab.openrtb.v2.json.OpenRtbJsonUtils.getCurrentName;
import static com.iabtechlab.openrtb.v2.json.OpenRtbJsonUtils.startArray;
import static com.iabtechlab.openrtb.v2.json.OpenRtbJsonUtils.startObject;

public class TransparencyJsonReader {

    public OpenRtbExt.Transparency readTransparency(JsonParser jsonParser) throws IOException {
        OpenRtbExt.Transparency.Builder builder = OpenRtbExt.Transparency.newBuilder();

        for (startObject(jsonParser); endObject(jsonParser); jsonParser.nextToken()) {
            final String fieldName = getCurrentName(jsonParser);
            if (jsonParser.nextToken() != JsonToken.VALUE_NULL) {
                readTransparencyFields(builder, jsonParser, fieldName);
            }
        }

        return builder.build();
    }

    private void readTransparencyFields(OpenRtbExt.Transparency.Builder builder,
                                        JsonParser jsonParser,
                                        String fieldName) throws IOException {
        switch (fieldName) {
            case "domain":
                builder.setDomain(jsonParser.getText());
                break;
            case "params":
                if (jsonParser.isExpectedStartArrayToken()) {
                    for (startArray(jsonParser); endArray(jsonParser); jsonParser.nextToken()) {
                        builder.addParams(jsonParser.getIntValue());
                    }
                }
                break;
        }
    }
}