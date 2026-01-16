package com.mikaelsonbraz.parkingapi.api.order.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.mikaelsonbraz.parkingapi.api.customer.model.entity.Customer;
import com.mikaelsonbraz.parkingapi.api.order.dto.OrderDTO;
import com.mikaelsonbraz.parkingapi.api.order.model.entity.Order;
import com.mikaelsonbraz.parkingapi.api.order.service.OrderService;
import com.mikaelsonbraz.parkingapi.api.parkingSpace.model.entity.ParkingSpace;
import com.mikaelsonbraz.parkingapi.api.renting.model.entity.Renting;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.BDDMockito;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

import java.time.LocalDateTime;
import java.util.Optional;

@ExtendWith(SpringExtension.class)
@ActiveProfiles("test")
@AutoConfigureMockMvc
@WebMvcTest(controllers = OrderController.class)
public class OrderControllerTest {

    static String ORDER_API = "/api/orders";

    @Autowired
    MockMvc mvc;

    @MockBean
    OrderService service;

    ObjectMapper objectMapper = new ObjectMapper()
            .registerModule(new JavaTimeModule());

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

    public OrderDTO createOrderDTO(){
        return OrderDTO.builder().idOrder(1).customer(createCustomer()).renting(createRenting()).build();
    }

    @Test
    @DisplayName("Must create a customer and return a customerDTO")
    public void createOrderTesst() throws Exception{
        //cenário
        OrderDTO orderDTO = createOrderDTO();
        Order order = createOrder();
        BDDMockito.given(service.save(Mockito.any(Order.class))).willReturn(order);
        String json = objectMapper.writeValueAsString(orderDTO);

        //execução
        MockHttpServletRequestBuilder request = MockMvcRequestBuilders
                .post(ORDER_API)
                .accept(MediaType.APPLICATION_JSON)
                .contentType(MediaType.APPLICATION_JSON)
                .content(json);

        //verificação
        mvc.perform(request)
                .andExpect(MockMvcResultMatchers.status().isCreated())
                .andExpect(MockMvcResultMatchers.jsonPath("idOrder").value(orderDTO.getIdOrder()))
                .andExpect(MockMvcResultMatchers.jsonPath("customer").value(orderDTO.getCustomer()));
    }

    @Test
    @DisplayName("Must get a order by id")
    public void getOrderTest() throws Exception{
        //cenário
        Order order = createOrder();
        BDDMockito.given(service.getById(order.getIdOrder())).willReturn(Optional.of(order));

        //execução
        MockHttpServletRequestBuilder request = MockMvcRequestBuilders
                .get(ORDER_API.concat("/" + order.getIdOrder()))
                .accept(MediaType.APPLICATION_JSON);

        //verificação
        mvc.perform(request)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("idOrder").value(order.getIdOrder()));
    }

    @Test
    @DisplayName("Must throw Exception when not found an order by id")
    public void orderNotFoundTest() throws Exception{
        //cenário
        BDDMockito.given(service.getById(Mockito.anyInt())).willReturn(Optional.empty());

        //execução
        MockHttpServletRequestBuilder request = MockMvcRequestBuilders
                .get(ORDER_API.concat("/1"))
                .accept(MediaType.APPLICATION_JSON);

        //verificação
        mvc.perform(request)
                .andExpect(MockMvcResultMatchers.status().isNotFound());
    }

    @Test
    @DisplayName("Must update an order")
    public void updateOrderTest() throws Exception{
        //cenário
        OrderDTO updatedOrderDTO = OrderDTO.builder().idOrder(1).customer(Customer.builder().idCustomer(2).build()).renting(Renting.builder().idRenting(2).build()).build();
        Order order = createOrder();
        BDDMockito.given(service.getById(Mockito.anyInt())).willReturn(Optional.of(order));
        String json = new ObjectMapper().writeValueAsString(updatedOrderDTO);

        //execução
        MockHttpServletRequestBuilder request = MockMvcRequestBuilders
                .put(ORDER_API.concat("/" + updatedOrderDTO.getIdOrder()))
                .accept(MediaType.APPLICATION_JSON)
                .contentType(MediaType.APPLICATION_JSON)
                .content(json);

        //verificação
        mvc.perform(request)
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andExpect(MockMvcResultMatchers.jsonPath("idOrder").value(updatedOrderDTO.getIdOrder()))
                .andExpect(MockMvcResultMatchers.jsonPath("customer").isEmpty())
                .andExpect(MockMvcResultMatchers.jsonPath("renting").isEmpty());
    }

    @Test
    @DisplayName("Update invalid order test")
    public void updateInvalidOrderTest() throws Exception{
        //cenário
        OrderDTO orderDTO = new OrderDTO();
        BDDMockito.given(service.getById(Mockito.anyInt())).willReturn(Optional.empty());
        String json = new ObjectMapper().writeValueAsString(orderDTO);

        //execução
        MockHttpServletRequestBuilder request = MockMvcRequestBuilders
                .put(ORDER_API.concat("/1"))
                .accept(MediaType.APPLICATION_JSON)
                .content(json);

        //verificação
        mvc.perform(request)
                .andExpect(MockMvcResultMatchers.status().isNotFound());
    }

    @Test
    @DisplayName("Must delete a order by id")
    public void deleteOrderTest() throws Exception{
        //cenário
        Order order = createOrder();
        BDDMockito.given(service.getById(Mockito.anyInt())).willReturn(Optional.of(order));

        //execução
        MockHttpServletRequestBuilder request = MockMvcRequestBuilders
                .delete(ORDER_API.concat("/1"));

        //execução
        mvc.perform(request)
                .andExpect(MockMvcResultMatchers.status().isNoContent());
    }

    @Test
    @DisplayName("Must throw ResponseStatusException when not found order by id to delete")
    public void deleteInvalidOrderTest() throws Exception{
        //cenário
        BDDMockito.given(service.getById(Mockito.anyInt())).willReturn(Optional.empty());

        //execução
        MockHttpServletRequestBuilder request = MockMvcRequestBuilders
                .delete(ORDER_API.concat("/1"));

        //execução
        mvc.perform(request)
                .andExpect(MockMvcResultMatchers.status().isNotFound());
    }
}