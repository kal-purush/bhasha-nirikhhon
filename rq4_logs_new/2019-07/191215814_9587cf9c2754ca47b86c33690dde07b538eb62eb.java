package com.epam.brest2019.courses.scale.impl;

import com.epam.brest2019.courses.scale.ValueScale;

import java.math.BigDecimal;
import java.util.Map;

public class PropertiesDistanceScale implements ValueScale<String, BigDecimal> {
    @Override
    public BigDecimal getValue(Map<String, BigDecimal> map, BigDecimal distance) {

        if (distance.doubleValue() <  10){
            return map.get("less10");
        }else if (distance.doubleValue() >=  10 && distance.doubleValue() < 100) {
            return map.get("less100");
        }else if (distance.doubleValue() >=  100 && distance.doubleValue() < 500) {
            return map.get("less500");
        }else if (distance.doubleValue() >=  500 && distance.doubleValue() < 1000) {
            return map.get("less1000");
        }else return map.get("more1000");
    }
}