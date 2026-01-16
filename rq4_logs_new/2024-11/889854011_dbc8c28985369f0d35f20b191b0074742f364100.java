package com.walking.intensive.chapter4.task16;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class Task16Test {

    @ParameterizedTest
    @MethodSource("isEqualSizeSource")
    void isEqualSizeTest(int[] arr1, int[] arr2, boolean expected) {
        boolean result = Task16.isEqualSize(arr1, arr2);
        assertEquals(expected, result);
    }

    @ParameterizedTest
    @MethodSource("isEqualsSource")
    void isEqualsTest(int[] arr1, int[] arr2, boolean expected) {
        boolean result = Task16.isEquals(arr1, arr2);
        assertEquals(expected, result);
    }

    @ParameterizedTest
    @MethodSource("incrementEachSource")
    void incrementEachTest(int[] arr, int[] expected) {
        int[] result = Task16.incrementEach(arr);
        assertArrayEquals(expected, result);
    }

    @ParameterizedTest
    @MethodSource("multiplyEachSource")
    void multiplyEachTest(int[] arr1, int[] arr2, int[] expected) {
        int[] result = Task16.multiplyEach(arr1, arr2);
        assertArrayEquals(expected, result);
    }

    @ParameterizedTest
    @MethodSource("subtractEachSource")
    void subtractEachTest(int[] arr1, int[] arr2, int[] expected) {
        int[] result = Task16.subtractEach(arr1, arr2);
        assertArrayEquals(expected, result);
    }

    @ParameterizedTest
    @MethodSource("reverseSource")
    void reverseTest(int[] arr, int[] expected) {
        int[] result = Task16.reverse(arr);
        assertArrayEquals(expected, result);
    }

    @ParameterizedTest
    @MethodSource("addSource")
    void addTest(int[] arr, int index, int newValue, int[] expected) {
        int[] result = Task16.add(arr, index, newValue);
        assertArrayEquals(expected, result);
    }

    @ParameterizedTest
    @MethodSource("isContainsSource")
    void isContainsTest(int[] arr, int value, boolean expected) {
        boolean result = Task16.isContains(arr, value);
        assertEquals(expected, result);
    }

    @ParameterizedTest
    @MethodSource("getFirstIndexSource")
    void getFirstIndexTest(int[] arr, int value, int expected) {
        int result = Task16.getFirstIndex(arr, value);
        assertEquals(expected, result);
    }

    @ParameterizedTest
    @MethodSource("getLastIndexSource")
    void getLastIndexTest(int[] arr, int value, int expected) {
        int result = Task16.getLastIndex(arr, value);
        assertEquals(expected, result);
    }

    @ParameterizedTest
    @MethodSource("removeByIndexSource")
    void removeByIndexTest(int[] arr, int index, int[] expected) {
        int[] result = Task16.removeByIndex(arr, index);
        assertArrayEquals(expected, result);
    }

    @ParameterizedTest
    @MethodSource("removeAllSource")
    void removeAllTest(int[] expected, int[] arr, int... removingValues) {
        int[] result = Task16.removeAll(arr, removingValues);
        assertArrayEquals(expected, result);
    }

    @ParameterizedTest
    @MethodSource("isSimilarSource")
    void isSimilarTest(int[] arr1, int[] arr2, boolean expected) {
        boolean result = Task16.isSimilar(arr1, arr2);
        assertEquals(expected, result);
    }

    @ParameterizedTest
    @MethodSource("shiftIndexSource")
    void shiftIndexTest(int[] arr, int[] expected) {
        int[] result = Task16.shiftIndex(arr);
        assertArrayEquals(expected, result);
    }

    static Stream<Arguments> isEqualSizeSource() {
        return Stream.of(
                Arguments.of(new int[]{10, 11, 12}, new int[]{11, 16, 1}, true),
                Arguments.of(new int[]{}, new int[]{}, true),
                Arguments.of(new int[]{1, 2, 3, 4, 5, 6, 7, 0, 0}, new int[]{0, 0, 0, 0, 0, 0}, false),
                Arguments.of(new int[]{10, 11, 12}, new int[]{16}, false),
                Arguments.of(null, new int[]{}, false),
                Arguments.of(new int[]{}, null, false),
                Arguments.of(null, null, false)
        );
    }

    static Stream<Arguments> isEqualsSource() {
        return Stream.of(
                Arguments.of(new int[]{10, 11, 12}, new int[]{10, 11, 12}, true),
                Arguments.of(new int[]{1, 2, 3, 4, 5, 6, 7}, new int[]{1, 2, 3, 4, 5, 6, 7}, true),
                Arguments.of(new int[]{}, new int[]{}, true),
                Arguments.of(new int[]{10, 11, 12}, new int[]{16}, false),
                Arguments.of(new int[]{10, 20, 10}, new int[]{10, 20, 1}, false),
                Arguments.of(null, new int[]{}, false),
                Arguments.of(new int[]{}, null, false),
                Arguments.of(null, null, false)
        );
    }

    static Stream<Arguments> incrementEachSource() {
        return Stream.of(
                Arguments.of(new int[]{10, 11, 12}, new int[]{11, 12, 13}),
                Arguments.of(new int[]{14, 7, 19, 0, 8, 5, 7, 8, 1}, new int[]{15, 8, 20, 1, 9, 6, 8, 9, 2}),
                Arguments.of(new int[]{}, new int[]{}),
                Arguments.of(null, null)
        );
    }


    static Stream<Arguments> multiplyEachSource() {
        return Stream.of(
                Arguments.of(new int[]{2, 4, 6}, new int[]{2, 4, 6}, new int[]{4, 16, 36}),
                Arguments.of(new int[]{6, 0}, new int[]{7, 1}, new int[]{42, 0}),
                Arguments.of(new int[]{0, 0, 0, 0}, new int[]{10, 20, 30, 40}, new int[]{0, 0, 0, 0}),
                Arguments.of(new int[]{}, new int[]{2, 4, 6}, new int[]{2, 4, 6}),
                Arguments.of(new int[]{2, 4, 6}, new int[]{}, new int[]{2, 4, 6}),
                Arguments.of(new int[]{}, new int[]{}, new int[]{}),
                Arguments.of(null, new int[]{2, 4, 6}, new int[]{2, 4, 6}),
                Arguments.of(new int[]{2, 4, 6}, null, new int[]{2, 4, 6}),
                Arguments.of(null, null, null)
        );
    }

    static Stream<Arguments> subtractEachSource() {
        return Stream.of(
                Arguments.of(new int[]{2, 4, 6}, new int[]{2, 4, 6}, new int[]{0, 0, 0}),
                Arguments.of(new int[]{6, 0}, new int[]{7, 1}, new int[]{-1, -1}),
                Arguments.of(new int[]{0, 0, 0, 0}, new int[]{10, 20, 30, 40}, new int[]{-10, -20, -30, -40}),
                Arguments.of(new int[]{}, new int[]{2, 4, 6}, new int[]{2, 4, 6}),
                Arguments.of(new int[]{2, 4, 6}, new int[]{}, new int[]{2, 4, 6}),
                Arguments.of(new int[]{}, new int[]{}, new int[]{}),
                Arguments.of(null, new int[]{2, 4, 6}, new int[]{2, 4, 6}),
                Arguments.of(new int[]{2, 4, 6}, null, new int[]{2, 4, 6}),
                Arguments.of(null, null, null)
        );
    }

    static Stream<Arguments> reverseSource() {
        return Stream.of(
                Arguments.of(new int[]{10, 11, 12}, new int[]{12, 11, 10}),
                Arguments.of(new int[]{14, 7, 19, 0, 8, 5, 7, 8, 1}, new int[]{1, 8, 7, 5, 8, 0, 19, 7, 14}),
                Arguments.of(new int[]{0, 0, 0, 1}, new int[]{1, 0, 0, 0}),
                Arguments.of(new int[]{}, new int[]{}),
                Arguments.of(null, null)
        );
    }

    static Stream<Arguments> addSource() {
        return Stream.of(
                Arguments.of(new int[]{10, 11, 12}, 1, 300, new int[]{10, 300, 11, 12}),
                Arguments.of(new int[]{1, 2, 3, 4, 5, 6, 7, 8, 9}, 5, 55, new int[]{1, 2, 3, 4, 5, 55, 6, 7, 8, 9}),
                Arguments.of(new int[]{1, 2, 3}, 10, 300, new int[]{1, 2, 3, 300}),
                Arguments.of(new int[]{1, 2, 3}, -10, 300, new int[]{}),
                Arguments.of(null, -10, 300, new int[]{})
        );
    }

    static Stream<Arguments> isContainsSource() {
        return Stream.of(
                Arguments.of(new int[]{10, 11, 12}, 10, true),
                Arguments.of(new int[]{1, 2, 3, 4, 5, 2000, 30000}, 2000, true),
                Arguments.of(new int[]{10, 11, 12, 1, 0, -3, 7, 8}, 700, false),
                Arguments.of(new int[]{-1, 0, 7, 7, 4, 2, 7, 9, 10}, -7, false),
                Arguments.of(new int[]{}, 10, false),
                Arguments.of(null, 10, false)
        );
    }

    static Stream<Arguments> getFirstIndexSource() {
        return Stream.of(
                Arguments.of(new int[]{10, 11, 12, 10, 20, 30}, 10, 0),
                Arguments.of(new int[]{10, 11, 12, 10, 20, 30, 60, 100}, 100, 7),
                Arguments.of(new int[]{10, 11, 12, 10, 20, 30}, -10, -1),
                Arguments.of(new int[]{0, 0, 0, 0, 0, 0, 0, 1}, -1, -1),
                Arguments.of(new int[]{}, -10, -1),
                Arguments.of(null, -10, -1)
        );
    }

    static Stream<Arguments> getLastIndexSource() {
        return Stream.of(
                Arguments.of(new int[]{10, 11, 12, 10, 20, 30}, 10, 3),
                Arguments.of(new int[]{10, 10, 10, 10, 10, 30}, 10, 4),
                Arguments.of(new int[]{10, 11, 12, 10, 20, 30, 60, 100}, 100, 7),
                Arguments.of(new int[]{10, 11, 12, 10, 20, 30}, -10, -1),
                Arguments.of(new int[]{0, 0, 0, 0, 0, 0, 0, 1}, -1, -1),
                Arguments.of(new int[]{}, -10, -1),
                Arguments.of(null, -10, -1)
        );
    }

    static Stream<Arguments> removeByIndexSource() {
        return Stream.of(
                Arguments.of(new int[]{10, 11, 12}, 1, new int[]{10, 12}),
                Arguments.of(new int[]{10, 11, 12, 1, 2, 3, 4}, 5, new int[]{10, 11, 12, 1, 2, 4}),
                Arguments.of(new int[]{10, 11, 12}, 5, new int[]{10, 11, 12}),
                Arguments.of(new int[]{10, 11, 12}, -5, new int[]{}),
                Arguments.of(null, 5, new int[]{})
        );
    }

    static Stream<Arguments> removeAllSource() {
        return Stream.of(
                Arguments.of(new int[]{11, 12, 3, 4, 5, 6}, new int[]{10, 11, 12, 1, 2, 3, 4, 5, 6, 7},
                        new int[]{10, 1, 7, 2}),
                Arguments.of(new int[]{3, 4, 5, 6, 7}, new int[]{1, 2, 3, 4, 5, 6, 7}, new int[]{10, 1, 30, 2}),
                Arguments.of(new int[]{1, 2, 3, 4, 6, 7}, new int[]{1, 2, 3, 4, 5, 6, 7}, 5),
                Arguments.of(new int[]{1, 2, 3, 4, 5}, new int[]{1, 2, 3, 4, 5}, new int[]{10, 20, 30}),
                Arguments.of(new int[]{}, new int[]{}, new int[]{10, 20, 30}),
                Arguments.of(new int[]{1, 2, 3, 4, 5}, new int[]{1, 2, 3, 4, 5}, new int[]{}),
                Arguments.of(new int[]{}, null, new int[]{1, 2, 3})
        );
    }

    static Stream<Arguments> isSimilarSource() {
        return Stream.of(
                Arguments.of(new int[]{10, 11, 12, 3, 1, 2}, new int[]{1, 2, 3, 4, 10, 5, 11, 6, 12}, true),
                Arguments.of(new int[]{1, 3, 5, 7, 9}, new int[]{9, 7, 5, 3, 10, 1}, true),
                Arguments.of(new int[]{1, 3, 5, 7, 9}, new int[]{9, 7, 5, 3, 10}, false),
                Arguments.of(new int[]{1, 3, 5, 7, 9}, new int[]{2, 4, 6, 8, 10}, false),
                Arguments.of(new int[]{1, 3, 5, 7, 9}, new int[]{}, false),
                Arguments.of(new int[]{}, new int[]{9, 7, 5, 3, 10, 1}, false),
                Arguments.of(null, new int[]{9, 7, 5, 3, 10, 1}, false),
                Arguments.of(new int[]{1, 3, 5, 7, 9}, null, false)
        );
    }

    static Stream<Arguments> shiftIndexSource() {
        return Stream.of(
                Arguments.of(new int[]{10, 11, 12, 3, 1, 2}, new int[]{2, 10, 11, 12, 3, 1}),
                Arguments.of(new int[]{1, 2, 3, 4, -1, 0, 6}, new int[]{6, 1, 2, 3, 4, -1, 0}),
                Arguments.of(new int[]{}, new int[]{}),
                Arguments.of(null, new int[]{})
        );
    }

}