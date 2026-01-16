package com.company;

import java.util.Arrays;

// E = Element
public class DynamicArray <E>{
    private int size;
    private Object[] content;

    public DynamicArray(int capacity){
        content = new Object[capacity];
    }
    public DynamicArray(){
        this(10);
    }

    public int size(){
        return size;
    }

    public int capacity(){
        return content.length;
    }
    public void add(E value){
        if(size == content.length){
            reallocate();
            content[size++] = value;
        }
    }

    @SuppressWarnings("unchecked")
    public E getAt(int index){
        if(index < 0 || index >= size){
            throw new IndexOutOfBoundsException();
        }
        return (E)content[index];
    }

    public void setAt(int index, E value){
        if(index < 0 || index >= size){
            throw new IndexOutOfBoundsException();
        }
        content[index] =value;
    }

    private void reallocate(){
        content = Arrays.copyOf(content, size *2);
    }
}