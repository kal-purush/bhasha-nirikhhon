using PetFamily.Domain.Entities.SharedValueObjects;

namespace PetFamily.Domain.Entities.Volunteers;

public record VolunteerDetails
{
    private readonly List<Requisite> _requisites = [];
    
    private readonly List<SocialNetwork> _socialNetworks = [];

    public IReadOnlyList<Requisite> Requisites => _requisites;
    
    public IReadOnlyCollection<SocialNetwork> SocialNetworks => _socialNetworks;

    private VolunteerDetails() {}
    
    private VolunteerDetails(List<Requisite> requisites, List<SocialNetwork> socialNetworks)
    {
        _requisites = requisites;
        _socialNetworks = socialNetworks;   
    }

    public static VolunteerDetails Create(List<Requisite> requisites, List<SocialNetwork> socialNetworks)
    {
        var details = new VolunteerDetails(requisites, socialNetworks);
        
        return details;
    }
}