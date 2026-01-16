package classworkday7;

public class ObjectContainer {

    Object[] array = new Object[10];
    int empos = 0;


    public void add(Object incoming) {
        if (empos < array.length) {
            array[empos++] = incoming;

        }

    }

    public Object removeLast() {
        Object lastElement = null;
        if (empos != 0) {
            lastElement = array[empos - 1];
            array[empos-- - 1] = null;
        }
        return lastElement;
    }

    public boolean isEmpty() {
        return empos != 0;
    }

}