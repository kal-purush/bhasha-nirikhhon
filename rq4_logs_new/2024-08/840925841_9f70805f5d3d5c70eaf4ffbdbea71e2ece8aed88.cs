using PetFamily.Domain.Entities.Species;

namespace PetFamily.Domain.Entities.Pets;

public record SpeciesDetails
{
    private SpeciesDetails(SpeciesId speciesId, Guid breedId)
    {
        SpeciesId = speciesId;
        BreedId = breedId;
    }
    
    public SpeciesId SpeciesId { get; }
    
    public Guid BreedId { get; }

    public static SpeciesDetails Create(SpeciesId speciesId, Guid breedId)
    {
        var details = new SpeciesDetails(speciesId, breedId);
        
        return details;
    }
    
}