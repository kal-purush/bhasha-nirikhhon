package com.app.bookstore.backend.service;

import com.app.bookstore.backend.DTO.JsonResponseDTO;
import com.app.bookstore.backend.DTO.OrderDTO;
import com.app.bookstore.backend.mapper.OrderMapper;
import com.app.bookstore.backend.model.*;
import com.app.bookstore.backend.repository.*;
import com.app.bookstore.backend.serviceimpl.OrderServiceImpl;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.json.JsonbTester;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class OrderServiceTest
{
    @Mock
    private UserRepository userRepository;

    @Mock
    private BookRepository bookRepository;

    @Mock
    private OrderRepository orderRepository;

    @Mock
    private AddressRepository addressRepository;

    @Mock
    private CartRepository cartRepository;

    @InjectMocks
    private OrderServiceImpl orderService;

    @InjectMocks
    private OrderMapper orderMapper;

    private User user;
    private Address address;
    private Cart cart;
    private Order order;
    private OrderDTO orderDTO;
    private Book book;

    @BeforeEach
    public void init()
    {
        book=Book.builder()
                .cartBookQuantity(0)
                .bookId(1L)
                .bookName("Atomic Habits")
                .price(199.90)
                .bookLogo("Url")
                .author("James Clear")
                .description("Improve by 1%")
                .quantity(35)
                .build();

        address=Address.builder()
                .userId(1L)
                .state("Andhra")
                .city("Kadapa")
                .pinCode(516004)
                .addressId(1L)
                .build();
        cart=Cart.builder()
                .cartId(1L)
                .userId(1L)
                .book(book)
                .quantity(1)
                .totalPrice(1345.90).build();

        List<Address> addresses=new ArrayList<>();
        addresses.add(address);

        List<Cart> carts=new ArrayList<>();
        carts.add(cart);

        user=User.builder()
                .userId(1L)
                .firstName("Sai")
                .lastName("Chandu")
                .email("marri@gmail.com")
                .dob(LocalDate.of(2002,8,24))
                .password("saichandu@45")
                .role("USER")
                .carts(carts)
                .addresses(addresses)
                .registeredDate(LocalDate.now()).build();

        orderDTO=OrderDTO.builder()
                .price(2345.8)
                .quantity(3)
                .addressId(address.getAddressId())
                .build();

        order=Order.builder()
                .orderId(1L)
                .cancelOrder(false)
                .orderDate(LocalDate.now())
                .carts(carts)
                .addressId(orderDTO.getAddressId())
                .orderPrice(orderDTO.getPrice())
                .build();
    }

    @Test
    public void OrderService_PlaceOrder_MustPlaceOrder()
    {
        when(userRepository.findByEmail(Mockito.anyString())).thenReturn(Optional.of(user));
        when(addressRepository.findById(Mockito.anyLong())).thenReturn(Optional.of(address));
        when(cartRepository.saveAll(Mockito.any())).thenReturn(List.of(cart));
        when(orderRepository.save(Mockito.any(Order.class))).thenReturn(order);

        JsonResponseDTO responseDTO=orderService.placeOrder(user.getEmail(),orderDTO);
        System.out.println(responseDTO);
        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isTrue();
    }

    @Test
    public void OrderService_CancelOrder_MustCancelOrder()
    {
        when(orderRepository.findById(Mockito.anyLong())).thenReturn(Optional.of(order));
        when(bookRepository.findById(Mockito.anyLong())).thenReturn(Optional.of(book));
        when(cartRepository.save(Mockito.any(Cart.class))).thenReturn(cart);
        when(orderRepository.save(Mockito.any(Order.class))).thenReturn(order);

        JsonResponseDTO responseDTO=orderService.cancelOrder(user.getEmail(),order.getOrderId());
        System.out.println(responseDTO);
        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isTrue();
    }

    @Test
    public void OrderService_GetAllOrders_MustReturnAllOrders()
    {
        when(orderRepository.findAll()).thenReturn(List.of(order));

        JsonResponseDTO responseDTO=orderService.getAllOrders();
        System.out.println(responseDTO);
        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isTrue();
        Assertions.assertThat(responseDTO.getData().size()).isEqualTo(1);
    }

    @Test
    public void OrderService_getAllOrdersForUser_MustReturnUserOrders()
    {
        when(userRepository.findByEmail(Mockito.anyString())).thenReturn(Optional.of(user));
        when(orderRepository.findByUserId(Mockito.anyLong())).thenReturn(List.of(order));

        JsonResponseDTO responseDTO=orderService.getAllOrdersForUser(user.getEmail());
        System.out.println(responseDTO);
        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isTrue();
        Assertions.assertThat(responseDTO.getData().size()).isEqualTo(1);
    }
}