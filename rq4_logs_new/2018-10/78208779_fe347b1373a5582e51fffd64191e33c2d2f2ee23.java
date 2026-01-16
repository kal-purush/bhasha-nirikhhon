package org.lxg.basic.jvm;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * @author: xuegangliu
 * @date: 10/25/2018 2:26 PM
 * @DES:
 * @version: v1.0
 */
public class JvmTest {

    private int size=1;

    public void stackleak(){
        size++;
        stackleak();
    }

    /**
     * OutOfMemoryError (堆异常)
     * VM arguments: -verbose:gc -Xms20M -Xmx20M -Xmn10M -XX:+PrintGCDetails -XX:SurvivorRatio=8
     */
    @Test
    public void OutOfMemoryErrorTest(){
        List<JvmTest> list=new ArrayList<JvmTest>();
        while(true){
            list.add(new JvmTest());
        }
    }

    /**
     * 虚拟机栈和方法栈异常
     * vm arguments : -Xss128k  （栈大小）
     */
    @Test
    public void stackOutOfTest(){
        JvmTest jvmTest=new JvmTest();
        jvmTest.stackleak();
    }


    /**
     * 运行时常量池溢出
     */
    @Test
    public void constantTest(){
        List<String> list=new ArrayList<String>();
        int i=0;
        while(true){
            list.add(String.valueOf(i++).intern());
        }  //intern() :若常量池没有当前的这个对象，则将此对相关加入常量池。
    }
}