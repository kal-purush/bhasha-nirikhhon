using PetFamily.Application.Dtos;
using PetFamily.Application.Volunteers.UpdatePet;

namespace PetFamily.API.Controllers.Volonteers.Requests
{
    public record UpdatePetRequest(
        string Name,
        string Description,
        string Color,
        string HealthInfo,
        Guid SpeciesId,
        Guid BreedId,
        AddressDto Address,
        int Weight,
        int Height,
        string PhoneNumber,
        bool IsCastrated,
        DateOnly BirthDate,
        bool IsVaccinated,
        string Status,
        IEnumerable<RequisiteDto> Requisites)
    {
        public UpdatePetCommand ToCommand(Guid volunteerId, Guid petId) =>
            new(
                volunteerId,
                petId,
                Name,
                Description,
                Color,
                HealthInfo,
                SpeciesId,
                BreedId,
                Address,
                Weight,
                Height,
                PhoneNumber,
                IsCastrated,
                BirthDate,
                IsVaccinated,
                Status,
                Requisites);
    }
}