package com.kodilla.ecommerce.mapper;

import com.kodilla.ecommerce.domain.Product;
import com.kodilla.ecommerce.dto.ProductDto;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;

import java.util.List;

@Mapper(componentModel = "spring")
public interface ProductMapper {

    @Mapping(target = "groupId", source = "group.groupId")
    ProductDto mapToDto(Product product);

    List<ProductDto> mapToDtos(List<Product> products);

    @Mapping(target = "group", ignore = true)
    Product map(ProductDto productDto);

}