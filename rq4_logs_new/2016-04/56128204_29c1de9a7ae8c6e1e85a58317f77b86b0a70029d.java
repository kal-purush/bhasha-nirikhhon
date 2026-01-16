/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.my.heap;

import static com.my.heap.Main.init;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author PC
 */
public class HeapTest {
    
    public HeapTest() {
    }

    @Test
    public void testIncreaseSize() {
        System.out.println("increaseSize");
        Heap h = new Heap(16);
        init(h);
        h.increaseSize();
        assertEquals(32, h.getMaxSize());
    }
    
    @Test
    public void testIsEmpty() {
        System.out.println("isEmpty");
        Heap h = new Heap(16);
        h.insert(22);
        h.remove();
        assertEquals(true, h.isEmpty());
    }
    
    @Test
    public void testInsert() {
        System.out.println("insert");
        Heap h = new Heap(16);
        h.insert(1);
        Node[] ex = {new Node(1)};
        Assert.assertArrayEquals(ex, h.getNodes());
    }

    @Test
    public void testRemove() {
        System.out.println("remove");
        Heap h = new Heap(16);
        h.insert(2);
        h.insert(1);
        h.remove();
        Node[] ex = {new Node(1)};
        Assert.assertArrayEquals(ex, h.getNodes());
    }

    @Test
    public void testChange() {
        System.out.println("change");
        Heap h = new Heap(16);
        h.insert(2);
        h.insert(3);
        h.change(1, 4);
        Node[] ex = {new Node(3), new Node(4)};
        Node[] is = {h.remove(), h.remove()};
        Assert.assertArrayEquals(ex, is);
    }
}