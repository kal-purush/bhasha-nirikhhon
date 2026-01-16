package com.kodilla.ecommercee.mapper;

import com.kodilla.ecommercee.domain.GroupNotFoundException;
import com.kodilla.ecommercee.domain.Product;
import com.kodilla.ecommercee.domain.dto.ProductDto;
import com.kodilla.ecommercee.service.DbGroupService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

@Component
@RequiredArgsConstructor
public class ProductMapper {
    private final GroupMapper groupMapper;
    private final DbGroupService dbGroupService;

    public Product mapToProduct(ProductDto productDto) throws GroupNotFoundException {
        return new Product(
                productDto.getProductName(),
                productDto.getProductType(),
                dbGroupService.getGroup(productDto.getGroupDto().getId()).orElseThrow(GroupNotFoundException::new));
    }

    public ProductDto mapToProductDto(Product product) {
        return new ProductDto(
                product.getId(),
                product.getProductName(),
                product.getProductType(),
                groupMapper.mapToGroupDto(product.getGroup()));
    }

    public List<ProductDto> mapTogProductDtoList(final List<Product> productList) {
        return productList.stream()
                .map(p -> new ProductDto(
                        p.getId(),
                        p.getProductName(),
                        p.getProductType(),
                        groupMapper.mapToGroupDto(p.getGroup())))
                .collect(Collectors.toList());
    }
}