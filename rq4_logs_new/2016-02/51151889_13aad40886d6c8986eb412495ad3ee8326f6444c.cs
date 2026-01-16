namespace BoardGames.Models
{
    using Data.Models;
    using System.ComponentModel.DataAnnotations;
    using Web.Infrastructure.Mapping;
    using AutoMapper;

    public class CategoryViewModel : IMapFrom<Category>, IHaveCustomMappings
    {
        public int Id { get; set; }

        [Required]
        [StringLength(30, ErrorMessage = "The {0} must be at least {2} characters long.", MinimumLength = 3)]
        public string Name { get; set; }

        public int NumberOfReviews { get; set; }

        public void CreateMappings(IMapperConfiguration configuration)
        {
            configuration.CreateMap<Category, CategoryViewModel>()
                .ForMember(x => x.NumberOfReviews, opt => opt.MapFrom(x => x.Reviews.Count));
        }
    }
}