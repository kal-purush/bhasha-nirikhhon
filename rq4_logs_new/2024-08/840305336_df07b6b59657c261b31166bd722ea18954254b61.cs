using ITPLibrary.Api.Core.Dtos;
using ITPLibrary.Api.Data.Entities;


namespace ITPLibrary.Api.Core.Profiles
{
    public class AuthorMaper
    {
        public AuthorDto ToDto(Author author)
        {
            return new AuthorDto()
            {
                Id = author.Id,
                Name = author.Name
                
            };
        }

        public Author ToEntity(AuthorDto authorDto)
        {
            return new Author()
            {
                Id = authorDto.Id,
                Name = authorDto.Name
            };
        }
    }
}