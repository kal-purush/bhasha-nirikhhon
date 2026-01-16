package com.epam.university.java.core.task003;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;

public class FlatMappingOperationImpl implements FlatMappingOperation {
    @Override
    public String[] flatMap(String source) {
        ArrayList<String> list = new ArrayList<>();
        source = source.replace(" ", "");
        list.addAll(Arrays.asList(source.split(", ")));
        HashSet<String> set = new HashSet<>(list);
        ArrayList<String> dlist = new ArrayList<>(set);
        String[] res = new String[dlist.size()];
        dlist.toArray(res);
        Arrays.sort(res);
        return res;
    }
}