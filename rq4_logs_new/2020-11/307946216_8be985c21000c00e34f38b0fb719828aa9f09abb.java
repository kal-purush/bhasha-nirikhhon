package hw.lesson2.lessonwork;

public interface Array<E> {

    void add(E value);

    void insert(E value, int index);

    E get(int index);

    boolean remove(E value);
    E remove(int index);

    int indexOf(E value);

    default boolean contains(E value) {
        return indexOf(value) != -1;
    }

    int size();

    default boolean isEmpty() {
        return size() == 0;
    }

    void display();

    void trimToSize();

    void sortBubble();
    void sortSelect();
    void sortInsert();

    E[] toArray();

    Array<E> copy();

}