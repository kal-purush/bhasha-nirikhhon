package com.kodilla.ecommerce.mapper;

import com.kodilla.ecommerce.domain.Cart;
import com.kodilla.ecommerce.dto.CartDto;
import com.kodilla.ecommerce.dto.CartItemDto;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
public class CartMapper {
//    public Cart mapToCart(final CartDto cartDto, List<CartItemDto> items) {
//        return new Cart(
//                cartDto.getItems(),
//                cartDto.getId(),
//                cartDto.getUserId());
//    }

    public CartDto mapToCartDto(final Cart cart) {
        return new CartDto(
                cart.getItems(),
                cart.getId(),
                cart.getUser().getId());
    }

}