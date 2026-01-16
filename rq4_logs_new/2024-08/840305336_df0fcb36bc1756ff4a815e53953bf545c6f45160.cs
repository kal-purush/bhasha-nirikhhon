using AutoMapper;
using ITPLibrary.Api.Core.Dtos;
using ITPLibrary.Api.Data.Entities;

namespace ITPLibrary.Api.Core.Profiles
{
    public class CategoryMapper
    {
        public CategoryMapper()
        {
            
           
        }
        public CategoryDto ToDto(Category category)
        {
            return new CategoryDto()
            {
                Id = category.Id,
                Name = category.Name
            };
        }

        public Category ToEntity(CategoryDto categoryDto)
        {
            return new Category()
            {
                Id = categoryDto.Id,
                Name = categoryDto.Name
            };
        }
    }
}