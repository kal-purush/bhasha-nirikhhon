import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class SplayTreeSortedSetTest {

    private final SortedSet<Integer> tree = fillTree();

    private SortedSet<Integer> fillTree() {
        final SortedSet<Integer> tree = new SplayTree<>();
        // В произвольном порядке добавим числа от 1 до 10
        tree.add(5);
        tree.add(1);
        tree.add(2);
        tree.add(7);
        tree.add(9);
        tree.add(10);
        tree.add(8);
        tree.add(4);
        tree.add(3);
        tree.add(6);
        return tree;
    }

    @Test
    public void headSetTest() {
        SortedSet<Integer> set = tree.headSet(5);
        assertEquals(true, set.contains(1));
        assertEquals(true, set.contains(2));
        assertEquals(true, set.contains(3));
        assertEquals(true, set.contains(4));
        assertEquals(false, set.contains(5));
        assertEquals(false, set.contains(6));
        assertEquals(false, set.contains(7));
        assertEquals(false, set.contains(8));
        assertEquals(false, set.contains(9));
        assertEquals(false, set.contains(10));

        set = tree.headSet(127);
        for (int i = 1; i <= 10; i++) {
            assertEquals(true, set.contains(i));
        }

        // Тесты на особые случаи
        // Случай, когда дерево пустое
        final SortedSet<Integer> splayTreeSet = new SplayTree<>();
        try {
            splayTreeSet.headSet(4);
            assert false;
        } catch (NoSuchElementException e) {
            System.out.println("Exception " + e + " successfully processed");
        }

        // Случай, когда пытаемся положить null как аргумент
        try {
            tree.headSet(null);
            assert false;
        } catch (NoSuchElementException e) {
            System.out.println("Exception " + e + " successfully processed");
        }

        // Случай, когда пытаемся вывать метод для узла, которого нет в дереве
        tree.add(17);
        tree.add(23);
        tree.add(21);
        tree.add(13);
        tree.add(18);
        tree.add(32);
        final SortedSet<Integer> newSet = tree.headSet(22);
        assertEquals(true, newSet.contains(1));
        assertEquals(true, newSet.contains(2));
        assertEquals(true, newSet.contains(3));
        assertEquals(true, newSet.contains(4));
        assertEquals(true, newSet.contains(5));
        assertEquals(true, newSet.contains(6));
        assertEquals(true, newSet.contains(7));
        assertEquals(true, newSet.contains(8));
        assertEquals(true, newSet.contains(9));
        assertEquals(true, newSet.contains(10));
        assertEquals(true, newSet.contains(13));
        assertEquals(true, newSet.contains(17));
        assertEquals(true, newSet.contains(18));
        assertEquals(true, newSet.contains(21));
        assertEquals(false, newSet.contains(23));
        assertEquals(false, newSet.contains(32));
    }

    @Test
    public void tailSetTest() {
        SortedSet<Integer> set = tree.tailSet(5);
        assertEquals(false, set.contains(1));
        assertEquals(false, set.contains(2));
        assertEquals(false, set.contains(3));
        assertEquals(false, set.contains(4));
        assertEquals(true, set.contains(5));
        assertEquals(true, set.contains(6));
        assertEquals(true, set.contains(7));
        assertEquals(true, set.contains(8));
        assertEquals(true, set.contains(9));
        assertEquals(true, set.contains(10));

        set = tree.tailSet(-128);
        for (int i = 1; i <= 10; i++) {
            assertEquals(true, set.contains(i));
        }

        // Тесты на особые случаи
        // Случай, когда дерево пустое
        final SortedSet<Integer> splayTreeSet = new SplayTree<>();
        try {
            splayTreeSet.tailSet(4);
            assert false;
        } catch (NoSuchElementException e) {
            System.out.println("Exception " + e + " successfully processed");
        }

        // Случай, когда пытаемся положить null как аргумент
        try {
            tree.tailSet(null);
            assert false;
        } catch (NoSuchElementException e) {
            System.out.println("Exception " + e + " successfully processed");
        }

        // Случай, когда пытаемся вывать метод для узла, которого нет в дереве
        tree.add(17);
        tree.add(23);
        tree.add(21);
        tree.add(13);
        tree.add(18);
        tree.add(32);
        final SortedSet<Integer> newSet = tree.tailSet(15);
        assertEquals(false, newSet.contains(1));
        assertEquals(false, newSet.contains(2));
        assertEquals(false, newSet.contains(3));
        assertEquals(false, newSet.contains(4));
        assertEquals(false, newSet.contains(5));
        assertEquals(false, newSet.contains(6));
        assertEquals(false, newSet.contains(7));
        assertEquals(false, newSet.contains(8));
        assertEquals(false, newSet.contains(9));
        assertEquals(false, newSet.contains(10));
        assertEquals(false, newSet.contains(13));
        assertEquals(true, newSet.contains(17));
        assertEquals(true, newSet.contains(18));
        assertEquals(true, newSet.contains(21));
        assertEquals(true, newSet.contains(23));
        assertEquals(true, newSet.contains(32));
    }

    @Test
    public void subSetTest() {
        SortedSet<Integer> set = tree.subSet(2, 9);
        assertEquals(false, set.contains(1));
        assertEquals(true, set.contains(2));
        assertEquals(true, set.contains(3));
        assertEquals(true, set.contains(4));
        assertEquals(true, set.contains(5));
        assertEquals(true, set.contains(6));
        assertEquals(true, set.contains(7));
        assertEquals(true, set.contains(8));
        assertEquals(false, set.contains(9));
        assertEquals(false, set.contains(10));

        set = tree.subSet(-128, 127);
        for (int i = 1; i <= 10; i++) {
            assertEquals(true, set.contains(i));
        }

        // Тесты на особые случаи
        // Случай, когда дерево пустое
        final SortedSet<Integer> splayTreeSet = new SplayTree<>();
        try {
            splayTreeSet.subSet(2, 4);
            assert false;
        } catch (NoSuchElementException e) {
            System.out.println("Exception " + e + " successfully processed");
        }

        // Случай, когда пытаемся положить null как аргумент
        try {
            tree.subSet(null, null);
            assert false;
        } catch (NoSuchElementException e) {
            System.out.println("Exception " + e + " successfully processed");
        }

        // Случай, когда пытаемся вывать метод для узла, которого нет в дереве
        tree.add(17);
        tree.add(23);
        tree.add(21);
        tree.add(13);
        tree.add(18);
        tree.add(32);
        final SortedSet<Integer> newSet = tree.subSet(11, 22);
        assertEquals(false, newSet.contains(1));
        assertEquals(false, newSet.contains(2));
        assertEquals(false, newSet.contains(3));
        assertEquals(false, newSet.contains(4));
        assertEquals(false, newSet.contains(5));
        assertEquals(false, newSet.contains(6));
        assertEquals(false, newSet.contains(7));
        assertEquals(false, newSet.contains(8));
        assertEquals(false, newSet.contains(9));
        assertEquals(false, newSet.contains(10));
        assertEquals(true, newSet.contains(13));
        assertEquals(true, newSet.contains(17));
        assertEquals(true, newSet.contains(18));
        assertEquals(true, newSet.contains(21));
        assertEquals(false, newSet.contains(23));
        assertEquals(false, newSet.contains(32));
    }

    @Test
    public void headSetRelationTest() {
        SortedSet<Integer> set = tree.headSet(7);
        assertEquals(6, set.size());
        assertEquals(10, tree.size());
        tree.add(0);
        assertTrue(set.contains(0));
        set.remove(4);
        assertFalse(tree.contains(4));
        tree.remove(6);
        assertFalse(set.contains(6));
        tree.add(12);
        assertFalse(set.contains(12));
        assertEquals(5, set.size());
        assertEquals(10, tree.size());

        // Тесты на особые случаи
        // Случай, когда добавляем крайний правый элемент
        assertFalse(set.contains(7));
        assertTrue(tree.contains(7));
        tree.remove(7);
        assertFalse(set.contains(7));
        assertFalse(tree.contains(7));
        tree.add(7);
        assertFalse(set.contains(7));
        assertTrue(tree.contains(7));

        // Случай, когда добавляем элемент в дерево через set
        assertFalse(set.contains(4));
        assertFalse(tree.contains(4));
        set.add(4);
        assertTrue(set.contains(4));
        assertTrue(tree.contains(4));

        // Проверка размеров дерева и множества после преобразований
        assertEquals(6, set.size());
        assertEquals(11, tree.size());
    }

    @Test
    public void tailSetRelationTest() {
        SortedSet<Integer> set = tree.tailSet(4);
        assertEquals(7, set.size());
        assertEquals(10, tree.size());
        tree.add(12);
        assertTrue(set.contains(12));
        set.remove(4);
        assertFalse(tree.contains(4));
        tree.remove(6);
        assertFalse(set.contains(6));
        tree.add(0);
        assertFalse(set.contains(0));
        assertEquals(6, set.size());
        assertEquals(10, tree.size());

        // Тесты на особые случаи
        // Проверка наличия минимального рарешённого элемента
        assertFalse(set.contains(4));

        // Проверка наличия элемента меньшего, чем минимальный рарешённый
        assertFalse(set.contains(3));

        // Случай, когда добавляем элемент в дерево через set
        set.add(38);
        assertTrue(set.contains(38));
        assertTrue(tree.contains(38));

        // Проверка размеров дерева и множества после преобразований
        assertEquals(7, set.size());
        assertEquals(11, tree.size());
    }

    @Test
    public void subSetRelationTest() {
        SortedSet<Integer> set = tree.subSet(4, 15);
        assertEquals(7, set.size());
        assertEquals(10, tree.size());
        tree.add(17);
        assertFalse(set.contains(17));
        set.remove(4);
        assertFalse(tree.contains(4));
        tree.remove(6);
        assertFalse(set.contains(6));
        tree.add(0);
        assertFalse(set.contains(0));
        assertEquals(5, set.size());
        assertEquals(10, tree.size());

        // Тесты на особые случаи
        // Случай, когда добавляем крайний правый элемент
        assertFalse(set.contains(15));
        assertFalse(tree.contains(15));
        tree.add(15);
        assertFalse(set.contains(15));
        assertTrue(tree.contains(15));

        // Случай, когда добавляем элемент в дерево через set
        set.add(14);
        assertTrue(set.contains(14));
        assertTrue(tree.contains(14));

        // Проверка размеров дерева и множества после преобразований
        assertEquals(6, set.size());
        assertEquals(12, tree.size());
    }
}