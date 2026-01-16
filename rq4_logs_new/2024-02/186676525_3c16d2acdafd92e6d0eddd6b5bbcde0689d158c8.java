package com.iabtechlab.openrtb.v2.json;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.iabtechlab.openrtb.v2.OpenRtbExt;

import static com.iabtechlab.openrtb.v2.json.OpenRtbJsonUtils.endObject;
import static com.iabtechlab.openrtb.v2.json.OpenRtbJsonUtils.getCurrentName;
import static com.iabtechlab.openrtb.v2.json.OpenRtbJsonUtils.startObject;

public class DsaResponseJsonReader {

    private final TransparencyJsonReader transparencyJsonReader;

    public DsaResponseJsonReader() {
        this.transparencyJsonReader = new TransparencyJsonReader();
    }

    public OpenRtbExt.DsaResponse.Builder read(JsonParser jsonParser) throws IOException {
        final OpenRtbExt.DsaResponse.Builder builder = OpenRtbExt.DsaResponse.newBuilder();
        for (startObject(jsonParser); endObject(jsonParser); jsonParser.nextToken()) {
            if (jsonParser.nextToken() != JsonToken.VALUE_NULL) {
                readDsaField(builder, jsonParser, getCurrentName(jsonParser));
            }
        }

        return builder;
    }

    private void readDsaField(OpenRtbExt.DsaResponse.Builder builder, JsonParser jsonParser, String fieldName)
            throws IOException {
        switch (fieldName) {
            case "behalf":
                builder.setBehalf(jsonParser.getValueAsString());
                break;
            case "paid":
                builder.setPaid(jsonParser.getValueAsString());
                break;
            case "transparency":
                builder.addAllTransparency(readTrancparency(jsonParser));
                break;
            case "adrender":
                int adRender = jsonParser.getValueAsInt(-1);
                if (adRender >= 0) {
                    builder.setAdrender(adRender == 1);
                }
        }
    }

    private List<OpenRtbExt.Transparency> readTrancparency(JsonParser jsonParser) throws IOException {
        List<OpenRtbExt.Transparency> transparencies = new ArrayList<>();

        JsonToken token = jsonParser.getCurrentToken();
        switch (token) {
            case START_ARRAY:
                for (OpenRtbJsonUtils.startArray(jsonParser); OpenRtbJsonUtils.endArray(jsonParser); jsonParser.nextToken()) {
                    transparencies.add(transparencyJsonReader.readTransparency(jsonParser));
                }
                break;
            case START_OBJECT:
                transparencies.add(transparencyJsonReader.readTransparency(jsonParser));
                break;
        }

        return transparencies;
    }
}