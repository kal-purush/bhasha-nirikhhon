package de.dhbw.ravensburg.hertel.w.Sorter;

import de.dhbw.ravensburg.hertel.w.Abstract.AbstractList;
import de.dhbw.ravensburg.hertel.w.Abstract.SortableList;

public class SimpleSorter<T> {

    private Number[] array;
    public AbstractList simpleSort(SortableList list) {
        if(!(list.getHead().getValue() instanceof Number))
            throw new IllegalArgumentException(list+"do" +
                    "es not contain numbers and is for this reason not sortable");
        array = (Number[])list.returnAsArray();
        list.removeAll();
        for (Number num :
                simpleSortTheInternalRealThingything()) {
            list.add((T) num);
        }
        return (AbstractList) list;
    }
    private Number[] simpleSortTheInternalRealThingything(){
        Number k;
        for (int i = array.length-1; i >= 1; i--) {
            for (int j = 0; j <= i - 1; j++) {
                if (array[j].doubleValue() >= array[i].doubleValue()) {
                    k = array[i];
                    array[i] = array[j];
                    array[j] = k;
                }
            }
        }
        return array;
    }
}