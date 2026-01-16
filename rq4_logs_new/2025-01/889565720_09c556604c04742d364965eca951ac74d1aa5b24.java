package com.app.bookstore.backend.service;

import com.app.bookstore.backend.DTO.CartRequestDTO;
import com.app.bookstore.backend.DTO.JsonResponseDTO;
import com.app.bookstore.backend.mapper.CartMapper;
import com.app.bookstore.backend.model.Book;
import com.app.bookstore.backend.model.Cart;
import com.app.bookstore.backend.model.User;
import com.app.bookstore.backend.repository.BookRepository;
import com.app.bookstore.backend.repository.CartRepository;
import com.app.bookstore.backend.repository.UserRepository;
import com.app.bookstore.backend.serviceimpl.CartServiceImpl;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.test.mock.mockito.MockBean;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CartServiceTest
{
    @Mock
    private CartRepository cartRepository;

    @Mock
    private BookRepository bookRepository;

    @Mock
    private UserRepository userRepository;

    @MockBean
    private CartMapper cartMapper;

    @InjectMocks
    private CartServiceImpl cartService;

    private User user;
    private Book book;
    private Cart cart;
    private CartRequestDTO cartRequestDTO;

    @BeforeEach
    public void init()
    {
        book=Book.builder()
                .bookId(1L)
                .bookName("Atomic Habits")
                .price(199.90)
                .bookLogo("Url")
                .author("James Clear")
                .description("Improve by 1%")
                .quantity(35)
                .cartBookQuantity(0)
                .build();

        cartRequestDTO=CartRequestDTO.builder()
                .bookId(book.getBookId())
                .quantity(1).build();

        cart=Cart.builder()
                .cartId(1L)
                .userId(1L)
                .book(book)
                .quantity(cartRequestDTO.getQuantity()).build();

        List<Cart> carts=new ArrayList<>();
        carts.add(cart);

        user=User.builder()
                .userId(1L)
                .firstName("Sai")
                .lastName("Chandu")
                .email("marri@gmail.com")
                .dob(LocalDate.of(2002,8,24))
                .password("saichandu@45")
                .carts(carts)
                .role("USER")
                .registeredDate(LocalDate.now()).build();
    }

    @Test
    public void CartService_AddToCart_MustAddBookToCart()
    {
        when(userRepository.findByEmail(Mockito.anyString())).thenReturn(Optional.of(user));
        when(bookRepository.findById(Mockito.anyLong())).thenReturn(Optional.of(book));
        when(cartRepository.save(Mockito.any(Cart.class))).thenReturn(cart);

        JsonResponseDTO responseDTO=cartService.addToCart(user.getEmail(),cartRequestDTO);
        System.out.println(responseDTO);
        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isTrue();
    }

    @Test
    public void CartService_RemoveFromCart_MustRemoveBookFromCart()
    {
        when(userRepository.findByEmail(Mockito.anyString())).thenReturn(Optional.of(user));
        when(bookRepository.findById(Mockito.anyLong())).thenReturn(Optional.of(book));
        when(cartRepository.findById(Mockito.anyLong())).thenReturn(Optional.of(cart));

        JsonResponseDTO responseDTO=cartService.removeFromCart(user.getEmail(),cart.getCartId());
        System.out.println(responseDTO);
        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isTrue();
    }

    @Test
    public void CartService_ClearCart_MustClearAllCarts()
    {
        when(userRepository.findByEmail(Mockito.anyString())).thenReturn(Optional.of(user));
        when(userRepository.save(Mockito.any(User.class))).thenReturn(user);
        when(bookRepository.save(Mockito.any(Book.class))).thenReturn(book);
        when(bookRepository.findById(Mockito.anyLong())).thenReturn(Optional.of(book));
        when(cartRepository.findByUserId(Mockito.anyLong())).thenReturn(List.of(cart));

        assertAll(()->cartService.clearCart(user.getEmail()));
    }

    @Test
    public void CartService_GetAllCarts_MustReturnAllCarts()
    {
        when(cartRepository.findAll()).thenReturn(List.of(cart));

        JsonResponseDTO responseDTO=cartService.getAllCarts();
        Assertions.assertThat(responseDTO.getData().size()).isEqualTo(1);
        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isTrue();
    }

    @Test
    public void CartService_GetUserCartById_MustReturnCart()
    {
        when(userRepository.findByEmail(Mockito.anyString())).thenReturn(Optional.of(user));
        when(cartRepository.findById(Mockito.anyLong())).thenReturn(Optional.of(cart));

        JsonResponseDTO responseDTO=cartService.getUserCartById(user.getEmail(),cart.getCartId());
        System.out.println(responseDTO);
        Assertions.assertThat(responseDTO).isNotNull();
        Assertions.assertThat(responseDTO.isResult()).isTrue();
    }
}