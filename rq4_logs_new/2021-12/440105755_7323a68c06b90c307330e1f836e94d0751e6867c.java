package com.atguigu.www.sync;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * ArrayList集合是不行的，如果多线程同时添加元素，有可能产生线程安全问题造成数据丢失，程序也有可能报错。
 * 例如：一个线知程正准备往进去写数据，突然切到另一个线程它先写了进入，在切回来这个线程并不道这个位置已经写入了数据，
 * 所以它还是会傻傻的写入数据，这样另一个线程的数据就被覆盖了。
 * 如果是一边添加 ，一边遍历的话程序会产生ConcurrentModificationException异常。
 */
@SuppressWarnings({"all"})
public class NotifyWaitDemo {
    public static void main(String[] args) {
        List<String> list = new ArrayList<>();
        for (int i = 0; i < 30; i++) {//i<3次数少时能明显看出数据丢失
            new Thread(()->{
                list.add(UUID.randomUUID().toString().substring(0,8));
                System.out.println(list);
            }, String.valueOf(i)).start();
        }
    }
}