package com;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.sf.json.JSONArray;

public class buildJson {
    public static void main(String[] args) throws IOException {
        File file = new File("C:/study/phpStudy/WWW/json.json");
        List<String> list = new lianJiaHouseDao().getAll();
        System.out.println(list);
        List<HashMap<String, String>> mMenuData = new ArrayList<>();
        for (int i = 0; i < list.size(); i=i+2) {
            HashMap<String, String> map = new HashMap<>();
            map.put("coord","[" + list.get(i) + "]");
            int a = i + 1;
            map.put("price", list.get(a));
            mMenuData.add(map);
        }
        net.sf.json.JSONArray jsonArray = net.sf.json.JSONArray.fromObject(mMenuData);
        System.out.println(jsonArray.toString());
        String s = jsonArray.toString();
        String regex1 = "\"(\\[)(\\d+)(\\.)(\\d+)(\\,)(\\d+)(\\.)(\\d+)(\\])\"";
        String regex2 = "\"(\\d+)\"";
        Pattern pattern1 = Pattern.compile(regex1);
        Pattern pattern2 = Pattern.compile(regex2);
        Matcher matcher1 = pattern1.matcher(s);
        Matcher matcher2 = pattern2.matcher(s);
        while (matcher1.find()) {
            String group1 = matcher1.group();
            s = s.replace(group1, group1.substring(1, group1.length() - 1));
        }
        while (matcher2.find()) {
            String group2 = matcher2.group();
            s = s.replace(group2, group2.substring(1, group2.length() - 1));
        }
        Writer write = new FileWriter(file);  
        write.write(s);  
        write.flush();  
        write.close();
    }
}