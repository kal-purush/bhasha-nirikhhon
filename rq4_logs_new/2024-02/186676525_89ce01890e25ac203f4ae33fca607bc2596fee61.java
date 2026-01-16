package com.iabtechlab.openrtb.v2.json;

import java.io.IOException;
import java.util.List;

import com.fasterxml.jackson.core.JsonGenerator;
import com.iabtechlab.openrtb.v2.OpenRtbExt;

public class TransparencyJsonWriter {

    public void writeTransparencies(List<OpenRtbExt.Transparency> transparencies, JsonGenerator gen) throws IOException {
        if (transparencies.isEmpty()) {
            return;
        }

        gen.writeArrayFieldStart("transparency");
        for (OpenRtbExt.Transparency transparency : transparencies) {
            writeTransparency(transparency, gen);
        }
        gen.writeEndArray();
    }

    private void writeTransparency(OpenRtbExt.Transparency transparency, JsonGenerator gen) throws IOException {
        gen.writeStartObject();
        writeTransparencyFields(transparency, gen);
        gen.writeEndObject();
    }

    private void writeTransparencyFields(OpenRtbExt.Transparency transparency, JsonGenerator gen) throws IOException {
        if (transparency.hasDomain()) {
            gen.writeStringField("domain", transparency.getDomain());
        }

        writeParams(transparency.getParamsList(), gen);
    }

    private void writeParams(List<Integer> params, JsonGenerator gen) throws IOException {
        if (params.isEmpty()) {
            return;
        }

        gen.writeArrayFieldStart("params");
        for (Integer param : params) {
            gen.writeNumber(param);
        }
        gen.writeEndArray();
    }
}