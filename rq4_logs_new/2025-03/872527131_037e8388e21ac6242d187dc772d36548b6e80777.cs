using System;
using Core.DTOs;
using Core.Entities;

namespace Core.Mapping;

public static class ProductMapping
{
    public static Product ToEntity(this CreateProductDto createProductDto)
    {
        return new Product()
        {
            Name = createProductDto.Name,
            Description = createProductDto.Description,
            ImageUrl = createProductDto.ImageUrl,
            CategoryId = createProductDto.CategoryId
        };
    }

    public static Product ToEntity(this UpdateProductDto updateProductDto, long id)
    {
        return new Product()
        {
            Id = id,
            Name = updateProductDto.Name,
            Description = updateProductDto.Description,
            Price = updateProductDto.Price,
            ImageUrl = updateProductDto.ImageUrl,
            CategoryId = updateProductDto.CategoryId
        };
    }

    
}