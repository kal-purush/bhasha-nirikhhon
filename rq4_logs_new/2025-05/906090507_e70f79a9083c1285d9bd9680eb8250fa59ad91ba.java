package org.dmytr.customer;

import org.dmytr.customer.dao.CustomerDao;
import org.dmytr.customer.dto.Customer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;

import java.util.List;

@SpringBootApplication
public class CustomerApplication {

    public static void main(String[] args) {

        ConfigurableApplicationContext applicationContext = SpringApplication.run(CustomerApplication.class, args);
        CustomerDao bean = applicationContext.getBean(CustomerDao.class);

        Customer customer = new Customer();
        customer.setFullName("Tom Hanks");
        customer.setEmail("tomhenks@gmail.com");
        customer.setSocialSecurityNumber(56789);
        bean.add(customer);

        Customer customer2 = new Customer();
        customer2.setFullName("Leonardo DiCaprio");
        customer2.setEmail("leo@gmail.com");
        customer2.setSocialSecurityNumber(12345);
        bean.add(customer2);

        Customer customer3 = new Customer();
        customer3.setFullName("John Rembo");
        customer3.setEmail("rembo@gmail.com");
        customer3.setSocialSecurityNumber(98760);
        bean.add(customer3);

        Customer byId = bean.findById(2);
        System.out.println(byId);

        List<Customer> all = bean.getAll();
        for (Customer customers : all) {
            System.out.println(customers);
        }

        Customer leonardoDiCaprio = bean.getNamed(2, "Leonardo DiCaprio");
        System.out.println(leonardoDiCaprio);

        bean.deleteById(3);

        Customer customer4 = new Customer();
        customer4.setId(1);
        customer4.setFullName("John Doe");
        customer4.setEmail("john.doe@gmail.com");
        customer4.setSocialSecurityNumber(654);
        bean.update(customer4);

    }

}