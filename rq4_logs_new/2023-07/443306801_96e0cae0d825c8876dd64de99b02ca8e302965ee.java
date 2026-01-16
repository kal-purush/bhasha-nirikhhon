package org.example.common.vo;

import com.alibaba.fastjson.annotation.JSONField;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * 动态 json 对象，使用方法参考测试用例 DynamicJsonObjectTest
 *
 * @author Albert
 * @version 1.0
 * @since 2023/6/30 11:41 AM
 */
@Data
public class DynamicJsonObject implements Serializable {
    private static final long serialVersionUID = 1L;

    @JsonIgnore
    @JSONField(unwrapped = true)
    private Map<String, Object> properties = new HashMap<>();

    /**
     * 获取所有 properties 以外声明的字段名
     */
    protected Set<String> fieldNames() {
        return Collections.emptySet();
    }

    /**
     * 通过 set() 添加字段到 properties 前进行校验
     *
     * @return true 可以添加, false 不可添加
     */
    protected boolean checkBeforeSet(String name, Object value) throws IllegalArgumentException {
        Set<String> set = fieldNames();
        if (set != null && set.contains(name)) {
            throw new IllegalArgumentException("please use setter method for field " + name + " in " + this.getClass().getSimpleName());
        }
        return true;
    }

    @JsonAnyGetter
    public Map<String, Object> any() {
        return this.properties;
    }

    @JsonAnySetter
    public void set(String name, Object value) {
        if (checkBeforeSet(name, value)) {
            this.properties.put(name, value);
        }
    }

    public Object get(String name) {
        return this.properties.get(name);
    }
}