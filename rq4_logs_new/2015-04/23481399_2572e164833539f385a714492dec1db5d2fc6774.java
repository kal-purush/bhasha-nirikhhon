package org.morningstarcc.morningstarapp.libs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Kyle on 4/25/2015.
 */
public abstract class SectionableList<T> {
    private List<T> data;
    private List<Integer> headerIndices;

    public SectionableList(T[] data, Comparator<T> sorter) {
        this.data = new LinkedList<>(Arrays.asList(data));
        this.headerIndices = new ArrayList<>();

        Collections.sort(this.data, sorter);

        this.data.add(0, buildHeader(this.data.get(0)));
        this.headerIndices.add(0);

        for (int i = 2; i < this.data.size(); i++) {
            if (sorter.compare(this.data.get(i), this.data.get(i - 1)) == 0) {
                this.data.add(i, buildHeader(this.data.get(i)));
                this.headerIndices.add(i);
                i++;
            }
        }
    }

    public abstract T buildHeader(T item);

    public boolean isHeader(int position) {
        int idx = Collections.binarySearch(headerIndices, position);

        return idx >= 0 && idx < headerIndices.size() && headerIndices.get(idx) == position;
    }

    public T get(int index) {
        return data.get(index);
    }

    public int size() {
        return data.size();
    }

    public String toString() {
        StringBuilder str = new StringBuilder();

        int i = 0;
        for (T s : data) {
            if (!isHeader(i))
                str.append('\t');

            str.append(s);
            str.append('\n');
            i++;
        }

        return str.toString();
    }
}