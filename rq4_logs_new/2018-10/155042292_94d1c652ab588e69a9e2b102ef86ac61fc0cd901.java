/*
 * 文件名：Test_20181028.java
 * 版权：Copyright 2017-2018 liutao. Co. Ltd. All Rights Reserved.
 * 修改人：Administrator
 * 修改时间：2018年10月28日
 * 修改内容：新建
 */
 
package test_20181028;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.Test;

/**
 * 〈一句话功能简述〉
 * 〈功能详细描述〉
 * @author Administrator
 * @version 1.0 2018年10月28日
 * @see Test_20181028
 * @since 1.0
 */
public class Test_20181028 {
    @Test
    public void testStringComparator() {
        List<String> list = new ArrayList<>();
        list.add("aaa");
        list.add("ZBBB");
        list.add("ccc");
        list.add("BBB");
//        Collections.sort(list, String.CASE_INSENSITIVE_ORDER);
        list.sort(String.CASE_INSENSITIVE_ORDER);

        System.out.println(list);
    }
}