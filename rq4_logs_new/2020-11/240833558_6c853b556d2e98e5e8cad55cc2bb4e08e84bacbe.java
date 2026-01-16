package com.niki.algorithm.arr;

import lombok.Data;

/**
 * @Description :给了个LNode的类，里面是链表的定义，来实现栈的pop函数功能
 * @Author : yarm.yang
 * @Date : 2020/11/20 9:39
*/
@Data
public class Lnode<T> {
    private T data;
    private Lnode next;
    private static int size;
    // 定义头结点
    private static Lnode head = new Lnode();

    /**
     * @Description :头部插入
     * @Author : yarm.yang
     * @Date : 2020/11/20 9:42
     * @Return :
    */
    public static void popHead(Lnode lnode){
        lnode.next = head.next;
        head.next = lnode;
    }

    /**
     * @Description :尾部插入
     * @Author : yarm.yang
     * @Date : 2020/11/20 9:42
     * @Return :
     */
    public static void popTail(Lnode lnode){
        // 找到尾部结点
        Lnode tail = head;
        while(tail.next != null){
            tail = tail.next;
        }
        tail.next = lnode;
    }
    
    /**
     * @Description :尾部结点
     * @Author : yarm.yang
     * @Date : 2020/11/20 10:51
     * @Return :
    */
    public static void forEch(){
        Lnode tail = head;
        while (tail.next != null){
            System.out.println(tail);
            tail = tail.next;
        }
    }

    public static void main(String[] args) {
        Lnode<String> node1 = new Lnode<>();
        Lnode<String> node2 = new Lnode<>();
        Lnode<String> node3 = new Lnode<>();
        Lnode<String> node4 = new Lnode<>();
        node1.setData("1");
        Lnode.popHead(node1);
        node2.setData("2");
        Lnode.popHead(node2);
        node3.setData("3");
        Lnode.popHead(node3);

        node4.setData("666666");
        popTail(node4);
        forEch();
    }
}