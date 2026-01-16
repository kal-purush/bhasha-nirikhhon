package com.maven_project.pizzas;

import com.maven_project.pizzas.domain.Address;
import com.maven_project.pizzas.domain.Customer;
import com.maven_project.pizzas.domain.Order;
import com.maven_project.pizzas.domain.Pizza;
import com.maven_project.pizzas.repository.card.JpaCardRepository;
import com.maven_project.pizzas.repository.order.JpaOrderRepository;
import com.maven_project.pizzas.repository.pizza.JpaPizzaRepository;
import com.maven_project.pizzas.service.card.AccumulativeCardService;
import com.maven_project.pizzas.service.discount.DiscountProvider;
import com.maven_project.pizzas.service.discount.DiscountService;
import com.maven_project.pizzas.service.order.OrderService;
import com.maven_project.pizzas.service.order.SimpleOrderService;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import java.util.Arrays;

/**
 * Created by Dennis
 *
 * on 4/26/2016.
 */
public class JpaPizzaApp {
    public static void main(String[] args) {
        EntityManagerFactory emf = null;
        EntityManager em = null;

        try {
            emf = Persistence.createEntityManagerFactory("jpa");
            em = emf.createEntityManager();

            JpaCardRepository cardRepository = new JpaCardRepository(em);
            JpaOrderRepository orderRepository = new JpaOrderRepository(em);
            JpaPizzaRepository pizzaRepository = new JpaPizzaRepository(em);

            OrderService orderService = new SimpleOrderService(orderRepository, pizzaRepository, new DiscountService(new DiscountProvider(new AccumulativeCardService(cardRepository))));

            Pizza pizza = new Pizza();
            pizza.setName("Marine pizza");
            pizza.setType(Pizza.Type.SEA);
            pizza.setCost(12.5);

            pizzaRepository.savePizza(pizza);

            Address address = new Address();
            address.setCity("Kiev");
            address.setStreet("Khrechatic 4");
            address.setAppartment("11");

            em.getTransaction().begin();
            em.persist(address);
            em.getTransaction().commit();

            Customer customer = new Customer();
            customer.setName("Valera");
            customer.setAddresses(Arrays.asList(address));

            em.getTransaction().begin();
            em.persist(customer);
            em.getTransaction().commit();

            Order order = orderService.placeNewOrder(customer, pizza.getId());

            System.out.println(order);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            em.close();
            emf.close();
        }
    }
}