/*
 * Copyright 2014-2015 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.dbflute.utflute.lastaflute.mock;

import java.util.List;
import java.util.Map;

import junit.framework.Assert;

/**
 * @author jflute
 */
public class TestingHtmlData {

    // ===================================================================================
    //                                                                           Attribute
    //                                                                           =========
    protected final Map<String, Object> dataMap; // not null

    // ===================================================================================
    //                                                                         Constructor
    //                                                                         ===========
    public TestingHtmlData(Map<String, Object> dataMap) {
        this.dataMap = dataMap;
    }

    // ===================================================================================
    //                                                                        Testing Tool
    //                                                                        ============
    public <VALUE> VALUE required(String key, Class<VALUE> valueType) {
        final Object value = dataMap.get(key);
        assertTrue(value != null);
        assertTrue(valueType.isAssignableFrom(value.getClass()));
        @SuppressWarnings("unchecked")
        final VALUE cast = (VALUE) value;
        return cast;
    }

    public <ELEMENT> List<ELEMENT> requiredList(String key, Class<ELEMENT> elementType) {
        @SuppressWarnings("unchecked")
        final List<ELEMENT> list = (List<ELEMENT>) dataMap.get(key);
        assertTrue(list != null);
        assertTrue(!list.isEmpty());
        assertTrue(elementType.isAssignableFrom(list.get(0).getClass()));
        return list;
    }

    protected void assertTrue(boolean condition) {
        Assert.assertTrue(condition);
    }

    // ===================================================================================
    //                                                                            Accessor
    //                                                                            ========
    public Map<String, Object> getDataMap() {
        return dataMap;
    }
}