package classwork.day10;

public class GenericContainer<T> {
    T[] array = (T[]) new Object[10];
    int empos = 0;


    public void add(T incoming) {
        if (empos < array.length) {
            array[empos++] = incoming;

        }

    }

    public T removeLast() {
        T lastElement = null;
        if (empos != 0) {
            lastElement = array[empos - 1];
            array[empos-- - 1] = null;
        }
        return lastElement;
    }

    public boolean isEmpty() {

        return empos == 0;
    }
}