using Microsoft.AspNetCore.Identity;
using PocketForzaHorizonCommunity.Back.Database.Entities.Guides;
using PocketForzaHorizonCommunity.Back.Database.Entities.UserStatistics;
using System.ComponentModel.DataAnnotations;

namespace PocketForzaHorizonCommunity.Back.Database.Entities;

public class ApplicationUser : IdentityUser<Guid>
{
    [Required]
    public GeneralStatistics GeneralStatistics { get; set; } = null!;
    [Required]
    public CampaignStatistics CampaignStatistics { get; set; } = null!;
    [Required]
    public OnlineStatistics OnlineStatistics { get; set; } = null!;
    [Required]
    public RecordStatistics RecordStatistics { get; set; } = null!;
    public ICollection<Tune> Tunes { get; set; } = new List<Tune>();
    public ICollection<Design> Designs { get; set; } = new List<Design>();
    public ICollection<OwnedCarsByUsers> OwnedCarsByUsers { get; set; } = new List<OwnedCarsByUsers>();
    public ICollection<UsersFriends> UsersFriends { get; set; } = new List<UsersFriends>();
}