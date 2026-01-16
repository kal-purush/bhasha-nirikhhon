package com.kodilla.ecommercee.mapper;

import com.kodilla.ecommercee.domain.Order;
import com.kodilla.ecommercee.domain.UserNotFoundException;
import com.kodilla.ecommercee.domain.dto.OrderDto;
import com.kodilla.ecommercee.service.DbUserService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class OrderMapper {

    private final DbUserService dbUserService;
    private final UserMapper userMapper;

    public Order mapToOrder(OrderDto orderDto) throws UserNotFoundException {
        return new Order(
                orderDto.getOrderName(),
                dbUserService.getUser(orderDto.getUserDto().getId()).orElseThrow(UserNotFoundException::new));
    }

    public OrderDto mapToOrderDto(Order order) {
        return new OrderDto(
                order.getId(),
                order.getOrderName(),
                userMapper.mapToUserDto(order.getUser()));
    }

    public List<OrderDto> mapToOrderDtoList(final List<Order> orderList) {
        return  orderList.stream()
                .map(o -> new OrderDto(
                        o.getId(),
                        o.getOrderName(),
                        userMapper.mapToUserDto(o.getUser())))
                .collect(Collectors.toList());
    }
}