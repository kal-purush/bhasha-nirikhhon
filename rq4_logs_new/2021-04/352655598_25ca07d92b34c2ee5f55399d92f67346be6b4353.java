package com.kodilla.ecommerce.mapper;

import com.kodilla.ecommerce.domain.Cart;
import com.kodilla.ecommerce.domain.CartItem;
import com.kodilla.ecommerce.domain.Product;
import com.kodilla.ecommerce.dto.CartDto;
import com.kodilla.ecommerce.dto.CartItemDto;
import com.kodilla.ecommerce.dto.ProductDto;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

@Mapper(componentModel = "spring")
public interface CartItemMapper {

    @Mapping(target = "product", ignore = true)
    @Mapping(target = "cart", ignore = true)
    CartItem mapToCartItem(final CartItemDto cartItemDto);

    @Mapping(target = "id", source = "cartItemDto.id")
    @Mapping(target = "price", source = "cartItemDto.price")
    @Mapping(target = "product", source = "product")
    @Mapping(target = "cart", source = "cart")
    CartItem mapToCartItem(final CartItemDto cartItemDto, final Cart cart, final Product product);

    @Mapping(target = "productId", source = "product.id")
    @Mapping(target = "cartId", source = "cart.id")
    CartItemDto mapToCartItemDto(final CartItem cartItem);
}