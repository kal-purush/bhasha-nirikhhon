package com.group33.greenthumb;

import com.activeandroid.serializer.TypeSerializer;

import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

final public class UtilMapSerializer extends TypeSerializer {
    @Override
    public Class<?> getDeserializedType() {
        return Map.class;
    }

    @Override
    public Class<?> getSerializedType() {
        return String.class;
    }

    @Override
    public String serialize(Object data) {
        if (data == null) {
            return null;
        }

        // Transform a Map<String, Object> to JSON and then to String
        return new JSONObject((Map<String, Object>) data).toString();
    }

    @Override
    public Map<String, Object> deserialize(Object data) {
        if (data == null) {
            return null;
        }

        // Properties of Model
        Map<String, Object> map = new HashMap<String, Object>();

        try {
            JSONObject json = new JSONObject((String) data);

            for(Iterator it = json.keys(); it.hasNext();) {
                String key = (String) it.next();

                map.put(key, json.get(key));
            }

        } catch (JSONException e) {
            e.printStackTrace();
        }

        return map;
    }
}