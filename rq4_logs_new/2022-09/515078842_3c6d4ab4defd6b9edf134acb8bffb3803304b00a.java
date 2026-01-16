package io.metadevs.akrasilnikov.module_2_9_DoubleLinkBilateralList;

public class ListIterator {
    private DoubleLink current;
    private DoubleLink previous;
    private DoubleLinkBilateralList ourList;

    public ListIterator(DoubleLinkBilateralList list) {
        ourList = list;
        reset();
    }

    public void reset() {
        current = ourList.getFirst();
        previous = null;
    }

    public boolean atEnd() {
        return (current.next == null);
    }

    public void nextLink() {
        previous = current;
        current = current.next;
    }

    public DoubleLink getCurrent() {
        return current;
    }

    public void insertAfter(long data) {
        DoubleLink newLink = new DoubleLink(data);
        if (ourList.isEmpty()) {
            ourList.setFirst(newLink);
            current = newLink;
        } else {
            newLink.next = current.next;
            current.next = newLink;
            nextLink();
        }
    }

    public void insertBefore(long data) {
        DoubleLink newLink = new DoubleLink<>(data);
        if (previous == null) {
            newLink.next = ourList.getFirst();
            ourList.setFirst(newLink);
            reset();
        } else {
            newLink.next = previous.next;
            previous.next = newLink;
            current = newLink;
        }
    }

    public long deleteCurrent() {
        long value = (long) current.data;
        if (previous == null) {
            ourList.setFirst(current.next);
            reset();
        } else {
            previous.next = current.next;
            if (atEnd()) {
                reset();
            } else {
                current = current.next;
            }
        }
        return value;
    }
}