package com.fqivy;

/**
 * @program: Array
 * @description:自己实现动态数组
 * @author: fqivy
 * @create: 2018-07-20 21:56
 */
public class Array<E> {
    /*数组*/
    private E[] data;

    /*数组元素个数*/
    private int size;

    /**
     * 可定义容量的构造方法
     *
     * @param capacity 数组初始容量
     */
    public Array(int capacity) {
        data = (E[]) new Object[capacity];
        size = 0;
    }

    /**
     * 默认无参数的构造方法
     */
    public Array() {
        this(10);
    }

    /**
     * 获取数组实际的元素个数
     *
     * @return 数组实际元素个数
     */
    public int getSize() {
        return size;
    }

    /**
     * 获取数组容量
     *
     * @return 数组容量
     */
    public int getCapacity() {
        return data.length;
    }

    /**
     * 向数组末尾插入数据
     *
     * @param e 插入的数据
     */
    public void addLast(E e) {
        if (size == data.length) {
            throw new IllegalArgumentException("Add argument failed！Array is full！");
        }
        data[size] = e;
        size++;
    }

    /**
     * 向指定的索引位置插入数据
     *
     * @param index 插入位置
     * @param e     待插入的元素
     */
    public void add(int index, E e) {
        if (index < 0 || index >= size) {
            throw new IllegalArgumentException("Add argument failed! Index is illegal!");
        }

        if (size == data.length) {
            resize(2 * data.length);
        }
        for (int i = size - 1; i >= index; i--) {
            data[i + 1] = data[i];
        }
        data[index] = e;
        size++;
    }


    /**
     * 向数组开头插入数据
     *
     * @param e 待插入元素
     */
    public void addFirst(E e) {
        add(0, e);
    }

    /**
     * 判断一个元素在数组中是否存在
     *
     * @param e 查询的元素
     * @return 存在返回true，不存在返回false
     */
    public boolean contains(E e) {
        for (int i = 0; i < size; i++) {
            if (e.equals(data[i])) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取数组索引处的元素值
     *
     * @param index
     * @return
     */
    public E get(int index) {
        if (index < 0 || index >= size) {
            throw new IllegalArgumentException("Get failed! Index is illegal!");
        }
        return data[index];
    }

    /**
     * 获取数组末尾的元素
     *
     * @return
     */
    public E getLast() {
        return get(size - 1);
    }

    /**
     * 判断数组是否为空
     * @return 为空返回true，不为空返回false
     */
    public boolean isEmpty() {
        return size == 0;
    }

    /**
     * 获取数组第一个元素
     *
     * @return
     */
    public E getFirst() {
        return get(0);
    }

    /**
     * 将索引处的元素值修改为e
     *
     * @param index 修改位置索引
     * @param e     修改后的元素值
     */
    public void set(int index, E e) {
        if (index < 0 || index >= size) {
            throw new IllegalArgumentException("Set failed! Index is illegal!");
        }
        data[index] = e;
    }

    /**
     * 查找元素所在索引
     *
     * @param e 查找元素
     * @return 第一个遇到的元素的索引，如果该元素不存在，返回-1
     */
    public int find(E e) {
        for (int i = 0; i < size; i++) {
            if (e.equals(data[i])) {
                return i;
            }
        }
        return -1;
    }

    /**
     * 删除制定索引位置的元素
     *
     * @param index 索引
     * @return 删除的元素值
     */
    public E remove(int index) {
        if (index < 0 || index >= size) {
            throw new IllegalArgumentException("Remove argument failed! Index is illegal!");
        }

        E ret = data[index];
        for (int i = index; i < size; i++) {
            data[i] = data[i + 1];
        }
        size--;

        if (size == data.length / 3) {
            resize(data.length / 2);
        }
        return ret;
    }

    /**
     * 删除数组的最后一个元素
     *
     * @return 删除的元素值
     */
    public E removeLast() {
        return remove(size - 1);
    }

    /**
     * 移除数组中的第一个元素
     *
     * @return 移除的元素值
     */
    public E removeFirst() {
        return remove(0);
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append(String.format("Array: size = %d ,capacity = %d \n", size, data.length));
        builder.append("[ ");
        for (int i = 0; i < size; i++) {
            builder.append(get(i));
            if (i != size - 1) {
                builder.append(" , ");
            }
        }
        builder.append(" ]");
        return builder.toString();
    }

    /**
     * 对数组大小进行调整
     *
     * @param newCapacity 新的数组大小
     */
    private void resize(int newCapacity) {
        E[] newArray = (E[]) new Object[newCapacity];
        for (int i = 0; i < size; i++) {
            newArray[i] = data[i];
        }
        data = newArray;
    }
}