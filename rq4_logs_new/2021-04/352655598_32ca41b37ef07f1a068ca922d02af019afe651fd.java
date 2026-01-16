package com.kodilla.ecommerce.service;

import com.kodilla.ecommerce.domain.Cart;
import com.kodilla.ecommerce.domain.Order;
import com.kodilla.ecommerce.domain.User;
import com.kodilla.ecommerce.dto.UserDto;
import com.kodilla.ecommerce.mapper.UserMapper;
import com.kodilla.ecommerce.repository.CartRepository;
import com.kodilla.ecommerce.repository.OrderRepository;
import com.kodilla.ecommerce.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class UserService {

    private final UserRepository userRepository;
    private final CartRepository cartRepository;
    private final OrderRepository orderRepository;
    private final UserMapper userMapper;

    public List<UserDto> getUsers() {
        return userRepository.findAll(Sort.by("name"))
                .stream()
                .map(userMapper::mapToDto)
                .collect(Collectors.toList());
    }

    public Optional<UserDto> getUserById(final Long id) {
        return userRepository.findById(id)
                .map(userMapper::mapToDto);
    }

    public UserDto saveUser(final UserDto userDto) {
        User user = userMapper.mapFromDto(userDto);
        Optional<Cart> cart = cartRepository.findByUser(user);
        if (cart.isPresent()){
            Cart userCart = cart.get();
            user.setCart(userCart);
        } else {
            user.setCart(Cart.builder()
                    .user(user)
                    .build());
        }

        List<Order> orders = orderRepository.findByUser(user);
        user.setOrders(orders);
        user = userRepository.save(user);
        return userMapper.mapToDto(user);
    }

    public Optional<UserDto> updateUser(final UserDto userDto) {
        Optional<User> user = userRepository.findById(userDto.getId());
        if (user.isPresent()) {
            user.ifPresent(u -> {
                u.setName(userDto.getName());
                Optional<Cart> cart = cartRepository.findById(userDto.getCartId());
                if (cart.isPresent()){
                    Cart userCart = cart.get();
                    u.setCart(userCart);
                }
                u.setOrders(orderRepository.findByUser(u));
                u.setStatus(userDto.getStatus());
                u.setUserKey(userDto.getUserKey());
            });
            return user.map(userMapper::mapToDto);
        }
        return Optional.empty();
    }


}