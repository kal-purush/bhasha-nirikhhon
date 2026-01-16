package org.whl.spring.aop.demo;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class App {

    public static void main(String[] args) {
        ApplicationContext appCtx = new ClassPathXmlApplicationContext("appContext.xml");
        PersonService pService = appCtx.getBean(PersonService.class);
        String pName = "Tom";
        pService.addPerson(pName);
        pService.deletePerson(pName);
        pService.editPerson(pName);
        ((ClassPathXmlApplicationContext)appCtx).close();
    }
}