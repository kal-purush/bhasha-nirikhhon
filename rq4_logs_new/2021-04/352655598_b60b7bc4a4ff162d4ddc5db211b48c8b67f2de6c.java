package com.kodilla.ecommerce.mapper;

import com.kodilla.ecommerce.domain.Group;
import com.kodilla.ecommerce.domain.Product;
import com.kodilla.ecommerce.dto.GroupDto;
import com.kodilla.ecommerce.dto.ProductDto;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@RequiredArgsConstructor
@Component
public class GroupMapper {

    public Group mapToGroup(final GroupDto groupDto) {

        List<Product> products = new ArrayList<>();

        return new Group(
                groupDto.getGroupId(),
                groupDto.getName(),
                products);
    }

    public GroupDto mapToGroupDto(final Group group) {
        List<Product> products = group.getProducts();
        List<ProductDto> productsDto = new ArrayList<>();
        for(Product product: products){
            productsDto.add(new ProductDto(product.getId(), product.getName(), product.getDescription(), product.getPrice(), product.getGroup().getGroupId()));
        }

        return new GroupDto(
                group.getGroupId(),
                group.getName(),
                productsDto);
    }

    public List<GroupDto> mapToGroupDtoList(final List<Group> groupList) {
        return groupList.stream()
                .map(this::mapToGroupDto)
                .collect(Collectors.toList());
    }

}