package com.yang.myProject.testAop;

import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * @author Yangjing
 */
public class AopTest {

    @Test
    public void testAop() {
        //很奇怪的是这里只能用这种方式获得Bean，如果用AutoWired来注入Dao就会报错
        //启动Spring容器
        ApplicationContext context = new ClassPathXmlApplicationContext("applicationContext.xml");
        //获取service组件,这里强制转换必须是接口而不是实现类
        Dao dao = (Dao) context.getBean("daoImpl");
        dao.insert();
        dao.update();
        dao.delete();
    }

}