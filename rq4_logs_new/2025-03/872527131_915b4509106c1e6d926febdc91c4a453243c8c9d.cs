using AutoMapper;
using Core.DTOs.Products.Requests;
using Core.DTOs.Products.Responses;
using Core.DTOs.Categories.Requests;
using Core.DTOs.Categories.Responses;
using Core.Entities;

namespace Core.Mapping;

public class MappingProfile : Profile
{
    public MappingProfile()
    {
        CreateMap<CreateProductDto, Product>();
        CreateMap<UpdateProductDto, Product>();
        CreateMap<Product, ProductListItemDto>();
        CreateMap<Product, ProductDetailsDto>()
            .ForMember(dest => dest.CategoryName, opt => opt.MapFrom(src => src.Category != null ? src.Category.Name : string.Empty));

        CreateMap<Category, CategoryDto>();
        CreateMap<CreateCategoryDto, Category>();
        CreateMap<UpdateCategoryDto, Category>();
    }
}