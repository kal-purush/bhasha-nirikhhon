namespace BoardGames.Areas.Admin.Models
{
    using System;
    using AutoMapper;
    using BoardGames.Web.Infrastructure.Mapping;
    using BoardGames.Data.Models;
    using System.ComponentModel.DataAnnotations;

    public class CommentViewModel : IMapFrom<Review>, IHaveCustomMappings
    {
        public int Id { get; set; }

        [Required]
        [MaxLength(200)]
        public string Content { get; set; }

        public string Author { get; set; }

        public int TutorialId { get; set; }

        public DateTime CreatedOn { get; set; }

        public DateTime ModifiedOn { get; set; }

        public void CreateMappings(IMapperConfiguration configuration)
        {
            configuration.CreateMap<Comment, CommentViewModel>()
                .ForMember(x => x.Author, opt => opt.MapFrom(x => x.Author.UserName));
        }
    }
}