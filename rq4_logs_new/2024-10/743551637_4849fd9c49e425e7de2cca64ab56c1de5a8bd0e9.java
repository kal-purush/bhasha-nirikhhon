package com.mango.studentmanager;

public class ArrayUtils {
    private ArrayUtils() {

    }

    public static int findIndexById(Student[] students, int id, int count) {
        for (int i = 0; i < count; i++) {
            if (id == students[i].getId()) {
                return i;
            }
        }
        return -1;
    }
}