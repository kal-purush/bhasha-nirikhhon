package ru.otus.java.basic.safarov.homework11;

import java.util.List;

public class FindTreeSortList implements SearchTree {
    private final List<Integer> list;

    public FindTreeSortList(List<Integer> list) {
        this.list = list;
    }


    @Override
    public Integer find(Object element) {
        getSortedList();
        if (element instanceof Integer) {
            int el = (Integer) element;
            if (search(0, list.size() - 1, el)) {
                return el;
            }

        }
        return null;
    }

    @Override
    public List<Integer> getSortedList() {
        int temp;
        for (int i = 0; i < list.size() - 1; i++) {
            for (int j = 0; j < list.size() - i - 1; j++) {
                if (list.get(j + 1) < list.get(j)) {
                    temp = list.get(j);
                    list.set(j, list.get(j + 1));
                    list.set(j + 1, temp);
                }
            }
        }
        return list;
    }

    private boolean search(int start, int end, int element) {
        if (list.get(start) == element || list.get(end) == element) {
            return true;
        }
        if (list.get(start) < element || list.get(end) > element) {
            return false;
        }
        if (list.get((end + start) / 2) > element) {
            start = (end + start) / 2;
        } else {
            end = (end + start) / 2;
        }
        return search(start, end, element);
    }

}