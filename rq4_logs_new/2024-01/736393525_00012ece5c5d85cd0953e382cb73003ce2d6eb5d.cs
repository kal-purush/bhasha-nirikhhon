using AutoMapper;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Data.Entities;
using BusinessLogic.Models.DTOs;

namespace BusinessLogic.Utils
{
    public class AutomapperProfile : Profile
    {
        public AutomapperProfile()
        {
            CreateMap<Genre, GenreDTO>()
                .ForMember(dest => dest.BookIds,
                    opt => opt.MapFrom(src => src.Books.Select(x => x.Id)));
            CreateMap<GenreDTO, Genre>()
                .ForMember(dest => dest.Books,
                    opt => opt.Ignore());
            CreateMap<Author, AuthorDTO>()
                .ForMember(dest => dest.BookIds,
                    opt => opt.MapFrom(src => src.Books.Select(x => x.Id)))
                .ForMember(dest => dest.FullName,
                    opt => opt.MapFrom(src => src.FirstName + " " + src.LastName));
            CreateMap<AuthorDTO, Author>()
                .ForMember(dest => dest.Books,
                    opt => opt.Ignore())
                .ForMember(dest => dest.FirstName,
                    opt => opt.MapFrom(src => src.FullName.Split().First()))
                .ForMember(dest => dest.LastName,
                    opt => opt.MapFrom(src => src.FullName.Split().Last()));
            CreateMap<Book, BookDTO>()
                .ForMember(dest => dest.UserIds,
                    opt => opt.MapFrom(src => src.Users.Select(x => x.Id)));
            CreateMap<BookDTO, Book>()
                .ForMember(dest => dest.Users,
                    opt => opt.Ignore());
            CreateMap<User, UserDTO>()
                .ForMember(dest => dest.BookIds,
                    opt => opt.MapFrom(src => src.Books.Select(x => x.Id)))
                .ForMember(dest => dest.FullName,
                    opt => opt.MapFrom(src => src.FirstName + " " + src.LastName));
            CreateMap<UserDTO, User>()
                .ForMember(dest => dest.Books,
                    opt => opt.Ignore())
                .ForMember(dest => dest.FirstName,
                    opt => opt.MapFrom(src => src.FullName.Split().First()))
                .ForMember(dest => dest.LastName,
                    opt => opt.MapFrom(src => src.FullName.Split().Last()));
        }
    }
}