using PetFamily.Accounts.Domain.DataModels;
using PetFamily.SharedKernel.ValueObjects;

namespace PetFamily.Accounts.Application.Dto;

public class UserInfoDto
{
    public FullName FullName { get; set; } 

    public string Photo { get; set; } = string.Empty;
    
    public string Email { get; set; } = string.Empty;
    
    public string PhoneNumber { get; set; } = string.Empty;
    
    public List<RoleDto> Roles { get; set; } = [];

    public List<SocialNetwork> SocialNetworks { get; set; } = [];

    public ParticipantAccountDto? ParticipantAccount { get; set; }
    
    public VolunteerAccountDto? VolunteerAccount { get; set; }
    
    public AdminAccountDto? AdminAccount { get; set; }
}