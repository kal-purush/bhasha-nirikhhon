package com.example.demo;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Optional.ofNullable;

public class MSConfigYamlDataSource implements MSConfigDataSourceI {
    private final Map<String, Object> yamlMap;

    public MSConfigYamlDataSource(Map<String, Object> yamlMap) {
        this.yamlMap = yamlMap;
    }

    @Override
    public Optional<Object> get(List<String> keyPath) {
        Object value = this.yamlMap;

        for (String key: keyPath) {
            if (value == null) {
                break;
            }

            if (value instanceof Map ) {
                value = ((Map<String, Object>) value).get(key);
            }
        }

        return ofNullable(value);
    }
}