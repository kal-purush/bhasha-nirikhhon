package com.kodilla.ecommerce.controller;

import com.kodilla.ecommerce.domain.enums.StatusOrder;
import com.kodilla.ecommerce.dto.OrderItemDto;
import com.kodilla.ecommerce.dto.OrderDto;
import com.kodilla.ecommerce.dto.ProductDto;
import org.springframework.web.bind.annotation.*;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping(value = "/v1/orders", consumes = APPLICATION_JSON_VALUE, produces = APPLICATION_JSON_VALUE)
public class OrderController {

    @GetMapping
    public List<OrderDto> getOrders() {
        ProductDto products = ProductDto.builder()
                .id(1L)
                .name("kurtka zimowa")
                .description("")
                .price(new BigDecimal(100))
                .groupId(1L)
                .build();

        List<OrderItemDto> orderItemDto = Arrays.asList(OrderItemDto.builder()
                .id(1L)
                .product(products)
                .quantity(3)
                .totalPrice(new BigDecimal(300))
                .build());

        return Arrays.asList(OrderDto.builder()
                .id(1L)
                .items(orderItemDto)
                .number("1")
                .status(StatusOrder.IN_PROGRESS)
                .shippingAddress("81-155 Gdynia, Kościuszki 1/2")
                .date(LocalDateTime.of(2021, 3, 1, 23, 23, 15))
                .userId(1L)
                .build());
    }

    @GetMapping ("/{id}")
    public OrderDto getOrder(@PathVariable Long id) {
        return OrderDto.builder()
                .id(1L)
                .number("1")
                .status(StatusOrder.ACCEPTED)
                .date(LocalDateTime.now())
                .build();
    }

    @PostMapping
    public OrderDto createOrder(@RequestBody OrderDto orderDto) {
        return OrderDto.builder()
                .id(1L)
                .number("1")
                .status(StatusOrder.ACCEPTED)
                .date(LocalDateTime.now())
                .build();
    }

    @PutMapping
    public OrderDto updateOrder(@RequestBody OrderDto orderDto) {
        return OrderDto.builder()
                .id(1L)
                .number("1")
                .status(StatusOrder.CANCELED)
                .date(LocalDateTime.now())
                .build();
    }

    @DeleteMapping("/{id}")
    public void deleteOrder(@PathVariable Long id) {
    }
}