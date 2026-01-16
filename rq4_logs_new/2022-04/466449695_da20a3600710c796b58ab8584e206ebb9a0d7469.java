package com.mikaelsonbraz.parkingapi.api.order.service;

import com.mikaelsonbraz.parkingapi.api.customer.model.entity.Customer;
import com.mikaelsonbraz.parkingapi.api.exceptions.BusinessException;
import com.mikaelsonbraz.parkingapi.api.order.model.entity.Order;
import com.mikaelsonbraz.parkingapi.api.order.model.repository.OrderRepository;
import com.mikaelsonbraz.parkingapi.api.order.service.orderServiceImpl.OrderServiceImpl;
import com.mikaelsonbraz.parkingapi.api.parkingSpace.model.entity.ParkingSpace;
import com.mikaelsonbraz.parkingapi.api.parkingSpace.model.spaceTypeENUM.SpaceType;
import com.mikaelsonbraz.parkingapi.api.renting.model.entity.Renting;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.TestEntityManager;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Optional;

@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
public class OrderServiceTest {

    OrderService service;

    @MockBean
    OrderRepository repository;

    @Autowired
    @MockBean
    TestEntityManager entityManager;

    public Customer createCustomer(){
        return Customer.builder().idCustomer(1).name("João").cpf("111.222.333-44").build();
    }

    public ParkingSpace createSpace(){
        return ParkingSpace.builder().idSpace(1).spaceType(1).occupied(true).build();
    }

    public Renting createRenting(){
        return Renting.builder().idRenting(1).entryDate(LocalDateTime.of(2022, 4, 4, 12, 30)).hourPrice(2).dayPrice(40).parkingSpace(createSpace()).build();
    }

    public Order createOrder(){
        return Order.builder().idOrder(1).customer(createCustomer()).renting(createRenting()).build();
    }

    @BeforeEach
    public void setUp() {
        this.service = new OrderServiceImpl(repository);
    }

    @Test
    @DisplayName("Must save a order")
    public void saveOrderTest(){
        //cenário
        Order order = createOrder();
        Mockito.when(repository.save(order)).thenReturn(order);

        //execução
        Order savedOrder = service.save(order);

        //verificação
        Assertions.assertThat(savedOrder.getIdOrder()).isNotNull();
        Assertions.assertThat(savedOrder.getCustomer().getIdCustomer()).isNotNull();
        Assertions.assertThat(savedOrder.getRenting().getIdRenting()).isNotNull();
        Assertions.assertThat(savedOrder.getRenting().getParkingSpace().getIdSpace()).isNotNull();
    }

    @Test
    @DisplayName("Must get a order by id")
    public void getByIdTest(){
        //cenário
        Order order = createOrder();
        Mockito.when(repository.findById(Mockito.anyInt())).thenReturn(Optional.of(order));

        //execução
        Optional<Order> foundOrder = service.getById(order.getIdOrder());

        //verificação
        Assertions.assertThat(foundOrder.isPresent()).isTrue();
    }

    @Test
    @DisplayName("Must return Optional.empty() when not find a orderr by id")
    public void orderNotFoundById(){
        //cenário
        Mockito.when(repository.findById(Mockito.anyInt())).thenReturn(Optional.empty());

        //execução
        Optional<Order> foundOrder = service.getById(1);

        //verificação
        Assertions.assertThat(foundOrder.isPresent()).isFalse();
    }

    @Test
    @DisplayName("Must update a order")
    public void updateOrderTest(){
        //cenário
        Order order = createOrder();
        Mockito.when(repository.save(Mockito.any(Order.class))).thenReturn(order);

        //execução
        Order updatedOrder = service.update(order);

        Assertions.assertThat(updatedOrder.getIdOrder()).isEqualTo(order.getIdOrder());
        Assertions.assertThat(updatedOrder.getCustomer()).isEqualTo(order.getCustomer());
        Assertions.assertThat(updatedOrder.getRenting()).isEqualTo(order.getRenting());
    }

    @Test
    @DisplayName("Must throw IllegalArgumentException when order is null trying update")
    public void updateInvalidOrderTest(){
        //cenário
        Order order = new Order();

        //execução

        //execução
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> service.update(order));
        Mockito.verify(repository, Mockito.never()).save(order);
    }

    @Test
    @DisplayName("Must delete a order")
    public void deleteOrderTest(){
        //cenário
        Order order = createOrder();
        entityManager.persist(order);

        //execução
        service.delete(order);
        Order deletedOrder = entityManager.find(Order.class, order);

        //verificação
        Mockito.verify(repository, Mockito.times(1)).delete(order);
        Assertions.assertThat(deletedOrder).isNull();
    }

    @Test
    @DisplayName("Must throw IllegalArgumentException when order id is null to delete")
    public void deleteInvalidOrderTest(){
        //cenário
        Order order = new Order();

        //execução

        //verificação
        org.junit.jupiter.api.Assertions.assertThrows(IllegalArgumentException.class, () -> service.delete(order));
        Mockito.verify(repository, Mockito.never()).delete(order);
    }

    @Test
    @DisplayName("Must update the order amount whe the order renting receives a departure date above 24h and unvered space type")
    public void updateOrderAmountTest(){
        //cenário
        Order order = createOrder();
        LocalDateTime hoje = LocalDateTime.of(2022, 4, 5, 12, 30);
        order.getRenting().setDepartureDate(hoje);
        Mockito.when(repository.save(Mockito.any(Order.class))).thenReturn(order);

        //execução
        Order updatedAmountOrder = service.updateAmount(order);

        //execução
        Assertions.assertThat(updatedAmountOrder.getIdOrder()).isNotNull();
        Assertions.assertThat(updatedAmountOrder.getAmount()).isEqualTo(32L);
    }

    @Test
    @DisplayName("Must update the order amount when the renting receives a departure date less 24h and covered space type")
    public void updateOrderAmountTest2(){
        //cenário
        Order order = createOrder();
        LocalDateTime hoje = LocalDateTime.of(2022, 4, 5, 11, 30);
        order.getRenting().setDepartureDate(hoje);
        order.getRenting().getParkingSpace().setSpaceType(SpaceType.COVERED);
        Mockito.when(repository.save(Mockito.any(Order.class))).thenReturn(order);

        //execução
        Order updatedAmountOrder = service.updateAmount(order);

        //verificação
        Assertions.assertThat(updatedAmountOrder.getIdOrder()).isNotNull();
        Assertions.assertThat(updatedAmountOrder.getAmount()).isEqualTo(46L);
    }

    @Test
    @DisplayName("Must throw BusinessException when trying update order amount with no departure date")
    public void updateInvalidOrderAmountTest(){
        //cenário
        Order order = createOrder();

        //execução

        //verificação
        org.junit.jupiter.api.Assertions.assertThrows(BusinessException.class, () -> service.updateAmount(order));
        Mockito.verify(repository, Mockito.never()).save(order);
    }
}