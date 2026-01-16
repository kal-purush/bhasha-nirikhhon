package com.aop.AopConcept;

import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import com.aop.AopConcept.service.PaymentService;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args )
    {
        System.out.println( "Hello World!" );
        
        ApplicationContext ac=new ClassPathXmlApplicationContext("com/aop/AopConcept/configFile.xml");
        PaymentService ps=ac.getBean("payment",PaymentService.class);
        
        ps.makePayment();
    }
}