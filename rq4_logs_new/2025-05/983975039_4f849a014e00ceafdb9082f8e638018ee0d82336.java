package org.acme.foodpackaging.domain;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ProductNameShortener {

    private final Map<String, String> productMap;
    public ProductNameShortener(){
        productMap = new HashMap<>();
        productMap.put("4810268043727", "Кокос-миндаль");
        productMap.put("4810268043710", "Аленка");
        productMap.put("4810268043475", "Фисташка");
        productMap.put("4810268040450", "Коровка");
        productMap.put("4810268047589", "ВарСгущенка");
        productMap.put("4810268047572", "Шоколадный");
        productMap.put("4810268054228", "Бискотти");
        productMap.put("4810268057748", "Тоффи");
        productMap.put("4810268058554", "Кактус");
        productMap.put("4810268055492", "Кофе-Карамель");
        productMap.put("4810268053870", "Картошка");
        productMap.put("4810268045042", "Ванильный");
        productMap.put("4810268050282", "ТвВаниль");
        productMap.put("4810268050671", "ТвСгущенка");
        productMap.put("4810268050640", "ТвШоколад");
        productMap.put("4810268050664", "ТвМанго");
        productMap.put("4810268050657", "ТпКлубника");
        productMap.put("4810268054969", "ТпФундук");
        productMap.put("4810268056826", "ТпКарамельАр");
        productMap.put("4810268050121", "ТпКлубника");
        productMap.put("4810268053153", "ТпМалина");
        productMap.put("4810268050138", "ТпШоколад");
        productMap.put("4810268044984", "вПлюш");
        productMap.put("4810268044977", "кПлюш");
    }

    public String getShortName(String ean13, String jobName) {
        return productMap.getOrDefault(ean13, getDefaultName(jobName));
    }

    private String getDefaultName(String jobName) {
        Pattern pattern = Pattern.compile("\"([^\"]+)\"");
        Matcher matcher = pattern.matcher(jobName);
        if (matcher.find()) {
            jobName = matcher.group(1); // Внутри кавычек
        }
        return jobName;
    }

    public Map<String, String> getAllMappings() {
        return productMap;
    }
}