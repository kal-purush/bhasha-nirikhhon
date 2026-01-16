
package com.apachescribe.utils;

import java.util.Map;

import org.json.JSONML;
import org.json.JSONObject;

public class XML_Json {

    private JSONObject getJsonFromXml(String req) {
        JSONObject res = null;

        // XML to map
        res = JSONML.toJSONObject(req);

        return res;
    }

    private String getXmlFromJson(Map<String, Object> req) {
        String res = null;
        JSONObject a = new JSONObject(req);

        // Map to XML
        res = JSONML.toString(a);

        return res;
    }

    // Tester: Use spring boot to test; wasting time debugging testing using strings
    // ):
    public static void main(String[] args) {
        // @PostMapping(path = { "/json-from-xml" })
        // private String json_from_xml(@RequestBody String req) {
        // return getJsonFromXml(req).toString();
        // }

        // @PostMapping(path = { "/xml-from-json" })
        // private String xml_from_json(@RequestBody Map req) {
        // return getXmlFromJson(req).toString();
        // }
    }
}