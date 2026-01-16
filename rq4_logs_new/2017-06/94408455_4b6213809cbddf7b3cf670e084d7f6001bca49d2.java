package com.ac.common.ajax;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;

import java.util.Map;

/**
 * Created by zhenchao.bi on 6/26/2017.
 */
public class AjaxResult {

    @JsonProperty
    private boolean result;

    @JsonProperty
    private Map<String, Object> data = Maps.newHashMap();

    @JsonIgnore
    public static AjaxResult success() {
        AjaxResult result = new AjaxResult();
        result.result = true;
        return result;
    }

    @JsonIgnore
    public static AjaxResult fail() {
        AjaxResult result = new AjaxResult();
        result.result = false;
        return result;
    }

    @JsonIgnore
    public AjaxResult addData(String key, Object value) {
        data.put(key, value);
        return this;
    }


}