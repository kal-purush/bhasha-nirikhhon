package com.github.rmannibucau.hazelcast.talend;

import static javax.json.JsonValue.ValueType.NUMBER;
import static javax.json.JsonValue.ValueType.STRING;
import static org.talend.sdk.component.api.component.Icon.IconType.CUSTOM;

import java.io.Serializable;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.json.JsonBuilderFactory;
import javax.json.JsonNumber;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.json.bind.Jsonb;

import org.talend.sdk.component.api.component.Icon;
import org.talend.sdk.component.api.component.Version;
import org.talend.sdk.component.api.configuration.Option;
import org.talend.sdk.component.api.processor.ElementListener;
import org.talend.sdk.component.api.processor.Processor;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

import lombok.Data;

@Data
@Version
@Icon(custom = "hazelcastOutput", value = CUSTOM)
@Processor(name = "Output")
public class HazelcastOutput implements Serializable {
    private final HazelcastConfiguration configuration;
    private final JsonBuilderFactory jsonFactory;
    private final Jsonb jsonb;
    private final HazelcastService service;

    private transient HazelcastInstance instance;
    private transient IMap<Object, Object> map;

    public HazelcastOutput(@Option("configuration") final HazelcastConfiguration configuration,
                           final JsonBuilderFactory jsonFactory,
                           final Jsonb jsonb,
                           final HazelcastService service) {
        this.configuration = configuration;
        this.jsonFactory = jsonFactory;
        this.jsonb = jsonb;
        this.service = service;
    }

    @PostConstruct
    public void init() {
        instance = service.findInstance(configuration.newConfig());
        map = instance.getMap(configuration.getMapName());
    }

    @ElementListener
    public void onElement(final JsonObject defaultInput) {
        final Object key = toValue(defaultInput.get(configuration.getKeyAttribute()));
        final Object value = toValue(defaultInput.get(configuration.getValueAttribute()));
        map.put(key, value);
    }

    @PreDestroy
    public void release() {
        instance.getLifecycleService().shutdown();
        map = null;
    }

    private Object toValue(final JsonValue jsonValue) {
        if (jsonValue == null) {
            return null;
        }
        if (jsonValue.getValueType() == STRING) {
            return JsonString.class.cast(jsonValue).getString();
        }
        if (jsonValue.getValueType() == NUMBER) {
            return JsonNumber.class.cast(jsonValue).doubleValue();
        }
        return jsonValue.asJsonObject();
    }
}