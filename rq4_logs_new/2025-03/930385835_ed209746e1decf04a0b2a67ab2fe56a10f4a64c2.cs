using Microsoft.EntityFrameworkCore;
using PetFamily.Application.Dto.Pet;
using PetFamily.Domain.PetContext.Entities;
using PetFamily.Domain.PetContext.ValueObjects.PetVO;
using PetFamily.Domain.PetContext.ValueObjects.VolunteerVO;
using PetFamily.Domain.Shared.SharedVO;
using PetFamily.Domain.SpeciesContext.Entities;
using PetFamily.Infrastructure.DbContexts;

namespace PetFamily.IntegrationTests.General;

public static class DataGenerator
{
    private static Volunteer CreateVolunteer(string suffix = "", int exp = 1)
    {
        var volunteerId = VolunteerId.NewVolunteerId();
        var email = Email.Create($"test{suffix}@test.com").Value;
        var fullName = FullName
            .Create($"testFirstName{suffix}", $"testLastName{suffix}", $"testSurname{suffix}").Value;
        var description = Description.Create($"testDescription{suffix}").Value;
        var experience = Experience.Create(exp).Value;
        var phoneNumber = Phone.Create("1-2-333-44-55-66").Value;
        var socialNetworksList = new List<SocialNetwork>();
        var transferDetailsList = new List<TransferDetail>();

        var volunteer = new Volunteer(
            volunteerId,
            fullName,
            email,
            description,
            experience,
            phoneNumber,
            socialNetworksList,
            transferDetailsList);

        return volunteer;
    }

    private static Pet CreatePet(Guid speciesId, Guid breedId, string suffix = "")
    {
        var petId = PetId.NewPetId();
        var name = Name.Create($"testName{suffix}").Value;
        var description = Description.Create($"testDescription{suffix}").Value;
        var petClassification = PetClassification
            .Create(speciesId, breedId).Value;
        var color = Color.Create($"testColor{suffix}").Value;
        var healthInfo = HealthInfo.Create($"testHealthInfo{suffix}").Value;
        var address = Address
            .Create($"testCity{suffix}", $"testStreet{suffix}", $"testApartment{suffix}").Value;
        var weight = Weight.Create(1).Value;
        var height = Height.Create(1).Value;
        var phoneNumber = Phone.Create("1-2-333-44-55-66").Value;
        var isCastrated = IsCastrated.Create(false).Value;
        var dateOfBirth = DateOfBirth.Create(DateTime.UtcNow.AddYears(-2)).Value;
        var isVaccinated = IsVaccinated.Create(false).Value;
        var helpStatus = HelpStatus.Create(PetStatus.InSearchOfHome).Value;
        var transferDetailsList = new List<TransferDetail>();
        var photosList = new List<Photo>();

        var pet = new Pet(
            petId,
            name,
            description,
            petClassification,
            color,
            healthInfo,
            address,
            weight,
            height,
            phoneNumber,
            isCastrated,
            dateOfBirth,
            isVaccinated,
            helpStatus,
            transferDetailsList,
            photosList);

        return pet;
    }

    public static Species CreateSpecies(string suffix = "")
    {
        var speciesId = Guid.NewGuid();
        var name = Name.Create($"test specie{suffix}").Value;
        var species = new Species(speciesId, name);

        return species;
    }

    public static Breed CreateBreed(string suffix = "")
    {
        var breedId = Guid.NewGuid();
        var name = Name.Create($"test breed{suffix}").Value;
        var breed = new Breed(breedId, name);

        return breed;
    }
    
    public static Volunteer CreateVolunteerWithPets(int count, Guid speciesId, Guid breedId)
    {
        var volunteer = CreateVolunteer();
        var pets = CreatePetList(count, speciesId, breedId);
        pets.ForEach(pet => volunteer.AddPet(pet));
        return volunteer;
    }
    
    public static List<Pet> CreatePetList(int count, Guid speciesId, Guid breedId)
    {
        var pets = Enumerable.Range(1, count).Select(_ => CreatePet(speciesId, breedId));
        return pets.ToList();
    }

    public static async Task<Guid> SeedVolunteer(WriteDbContext dbContext)
    {
        var volunteer = CreateVolunteer();
        dbContext.Volunteers.Add(volunteer);
        await dbContext.SaveChangesAsync();
        return volunteer.Id;
    }

    public static async Task<Guid> SeedSpecies(WriteDbContext dbContext)
    {
        var species = CreateSpecies();
        dbContext.Species.Add(species);
        await dbContext.SaveChangesAsync();

        return species.Id;
    }

    public static async Task<Guid> SeedBreed(WriteDbContext dbContext, Guid speciesId)
    {
        var species = await dbContext.Species.FirstOrDefaultAsync(s => s.Id == speciesId);

        var breed = CreateBreed();
        species!.AddBreed(breed);
        await dbContext.SaveChangesAsync();

        return breed.Id;
    }
    
    public static async Task<Guid> SeedVolunteerWithPets(
        WriteDbContext dbContext,
        int count,
        Guid speciesId,
        Guid breedId)
    {
        var volunteer = CreateVolunteerWithPets(count, speciesId, breedId);
        dbContext.Volunteers.Add(volunteer);
        await dbContext.SaveChangesAsync();
        
        return volunteer.Id;
    }
    
}