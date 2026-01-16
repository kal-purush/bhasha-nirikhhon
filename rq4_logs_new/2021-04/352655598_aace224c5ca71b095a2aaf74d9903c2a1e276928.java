package com.kodilla.ecommerce.mapper;

import com.kodilla.ecommerce.domain.OrderItem;
import com.kodilla.ecommerce.dto.OrderItemDto;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

import java.util.List;

@Mapper(componentModel = "spring")
public interface OrderItemMapper {

    @Mapping(target = "price", ignore = true)
    @Mapping(target = "quantity", ignore = true)
    @Mapping(target = "product.group", ignore = true)
    @Mapping(target = "order.user", ignore = true)
    OrderItem mapToOrderItem(final OrderItemDto orderItemDto);

    @Mapping(target = "group.groupId", ignore = true)
    @Mapping(target = "id", ignore = true)
    OrderItemDto mapToOrderItemDto(final OrderItem orderItem, final List<OrderItem> orderItemList);

    List<OrderItemDto> mapToOrderItemDtoList(final List<OrderItem> orderItemList);

    List<OrderItem> mapToOrderItemList(final List<OrderItemDto> orderItemDtoList);

}