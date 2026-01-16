package com.example.plaintest;

import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.util.function.Predicate;
import java.util.function.Supplier;


/**
 * Created by arahansa on 2016-04-17.
 */
public class EqualsTest {

    final static long times = 9000000000l;
    final static String testString = "aslkdfjlkasjzxjcklvajskldfjsa";

    @Test
    public void testEqual() throws Exception{
        System.out.println("람다로 돌린 테스트");
        testStringEquals(s -> StringUtils.equals(s, ""), "스트링유틸");
        testStringEquals(s -> s!=null && s.equals(""), "일반 equals");
    }

    private void testStringEquals(Predicate<String> stringPredicate, String testType){
        long before = System.currentTimeMillis();
        int j=0;
        for(long i=0;i<times;i++)
            if(stringPredicate.test(testString))
                j++;
        long after = System.currentTimeMillis();
        System.out.println(testType + " 걸린 시간(람다) :"+ (after-before));
    }

    @Test
    public void 이퀄시간_스트링유틸() throws Exception {
        long before = System.currentTimeMillis();
        int j=0;
        for(long i=0;i<times;i++)
            if(StringUtils.equals(testString, ""))
                j++;
        long after = System.currentTimeMillis();
        System.out.println("스트링유틸 " + " 걸린 시간 :"+ (after-before));
    }

    @Test
    public void 이퀄시간_일반자바() throws Exception {
        long before = System.currentTimeMillis();
        int j=0;
        for(long i=0;i<times;i++)
            if("".equals(testString))
                j++;
        long after = System.currentTimeMillis();
        System.out.println("일반 이퀄" + " 걸린 시간 :"+ (after-before));
    }


}