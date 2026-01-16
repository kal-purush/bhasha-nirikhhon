package application;

import java.util.Iterator;
import java.util.List;

public class EmployeeList implements Iterable<FermiEntry> {
    private List<FermiEntry> list = null;

    public EmployeeList(List<FermiEntry> list) {
        this.list = list;
    }

    @Override
    public Iterator<FermiEntry> iterator() {
        return new FermiEntryIterator<>(list);
    }
}

class FermiEntryIterator<FermiEntry> implements Iterator<FermiEntry> {

    int indexPosition = 0;
    List<FermiEntry> internalList;

    public FermiEntryIterator(List<FermiEntry> internalList) {
        this.internalList = internalList;
    }

    @Override
    public boolean hasNext() {
        if (internalList.size() >= indexPosition + 1) {
            return true;
        }
        return false;
    }

    @Override
    public FermiEntry next() {
        FermiEntry val = internalList.get(indexPosition);
        indexPosition += 1;
        return val;
    }
}