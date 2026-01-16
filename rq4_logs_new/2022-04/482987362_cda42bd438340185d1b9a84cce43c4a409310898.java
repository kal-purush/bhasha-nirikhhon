package ru.avdotchenkov.lesson_1.common;
public class CommonStorage implements Storage {
    private final Object[] data;
    private int currentSize;

    public CommonStorage(int size) {
        this.data = new Object[size];
    }

    public Object get(int index) {
        return data[index];
    }

    @Override
    public boolean contains(Object value) {
        for (int i = 0; i < currentSize; i++) {
            if (value.equals(data[i])) {
                return true;
            }
        }
        return false;
    }

    public void add(Object value) {
        add(value, currentSize);
    }

    public void add(Object value, int index) {
        data[index] = value;
        currentSize++;
    }

    public void remove(int index) {
        data[index] = "";
        currentSize--;
    }

    public void display() {
        for (int i = 0; i < data.length; i++) {
            System.out.println(data[i]);
        }
    }

}