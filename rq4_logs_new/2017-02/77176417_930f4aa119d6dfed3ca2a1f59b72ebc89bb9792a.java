package com.devblogs;

import java.util.Locale;
import org.springframework.context.MessageSource;
import org.springframework.context.support.GenericXmlApplicationContext;

public class App {
	public static void main(String[] args) {
		GenericXmlApplicationContext context = new GenericXmlApplicationContext();
		context.load("classpath:spring/context.xml");
		context.refresh();
		
		Locale english = Locale.ENGLISH;
		Locale russian = new Locale("ru", "RU");
		
		System.out.println("-------------------Получение MessageSource из контекста--------------------------");
		
		System.out.println(context.getMessage("msg", null, english));
		System.out.println(context.getMessage("msg", null, russian));
		
		System.out.println(context.getMessage("nameMsg", new Object[] { "Name1", "Name2"}, english));
		System.out.println(context.getMessage("nameMsg", new Object[] { "Name1", "Name2"}, russian));
		
		//----------Получаем доступ к бину ResourceBundleMessageSource-------------
		System.out.println("---------------------------Лукап ResourceBundleMessageSource-------------------------------");
		
		MessageSource messageSource = (MessageSource) context.getBean("messageSource");
		
		System.out.println(messageSource.getMessage("msg", null, english));
		System.out.println(messageSource.getMessage("msg", null, russian));
		
		System.out.println(messageSource.getMessage("nameMsg", new Object[] { "Name1", "Name2"}, english));
		System.out.println(messageSource.getMessage("nameMsg", new Object[] { "Name1", "Name2"}, russian));
	}
}