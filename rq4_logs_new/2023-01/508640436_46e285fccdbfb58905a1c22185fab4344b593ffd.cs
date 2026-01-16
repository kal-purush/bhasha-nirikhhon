using car_racing_tournament_api.Models;

namespace car_racing_tournament_api.DTO
{
    public class SeasonOutputDto
    {
        public Guid Id { get; set; }
        public string Name { get; set; } = default!;
        public string? Description { get; set; }
        public List<UserSeasonOutputDto> UserSeasons { get; set; } = default!;
    }

    public class UserSeasonOutputDto
    {
        public string Username { get; set; } = default!;
        public UserSeasonPermission Permission { get; set; }
    }
}