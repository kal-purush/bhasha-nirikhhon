package com.kodilla.ecommerce.mapper;

import com.kodilla.ecommerce.domain.Cart;
import com.kodilla.ecommerce.dto.CartDto;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

import java.util.List;


@Mapper(componentModel = "spring")
public interface CartMapper {

    @Mapping(target = "items", ignore = true)
    Cart mapToCart(CartDto cartDto);

    List<CartDto> mapToCartDtoList(final List<Cart> cartList);

    @Mapping(target = "items", ignore = true)
    CartDto mapToCartDto(Cart cart);

}