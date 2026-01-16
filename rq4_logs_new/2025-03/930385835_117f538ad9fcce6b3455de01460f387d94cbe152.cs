using System.Reflection;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using PetFamily.IntegrationTests.General;
using PetFamily.IntegrationTests.Volunteers.Heritage;
using PetFamily.Volunteers.Application.Commands.DeletePet;
using PetFamily.Volunteers.Infrastructure.Services;

namespace PetFamily.IntegrationTests.Volunteers.ServicesTests;


public class DeleteExpiredEntitiesServiceTests : VolunteerTestsBase
{
    private readonly DeleteExpiredEntitiesService _sut;
    public DeleteExpiredEntitiesServiceTests(VolunteerTestsWebFactory factory) : base(factory)
    {
        _sut = Scope.ServiceProvider.GetRequiredService<DeleteExpiredEntitiesService>();
    }
    
    [Fact]
    public async Task SoftDeletePet_success_should_delete_2_pets_that_were_soft_deleted_with_expired_lifetime()
    {
        // arrange
        var PET_COUNT = 5;
        var DAYS_BEFORE_DELETION = 0; // should be deleted immediately
        var volunteer = await DataGenerator.SeedVolunteerWithPets(VolunteersWriteDbContext, SpeciesWriteDbContext, PET_COUNT);
        var pet1 = volunteer.AllOwnedPets[0];
        var pet2 = volunteer.AllOwnedPets[3];
        volunteer.SoftDeletePet(pet1);
        volunteer.SoftDeletePet(pet2);
        
        // act
        await _sut.Process(DAYS_BEFORE_DELETION, CancellationToken.None);

        // assert
        volunteer.AllOwnedPets.Count.Should().Be(PET_COUNT - 2);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == pet1.Id).Should().BeNull();
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == pet2.Id).Should().BeNull();
    }
    
    [Fact]
    public async Task SoftDeletePet_success_should_delete_only_1_pet_from_2_soft_deleted_because_of_their_lifetime()
    {
        // arrange
        var PET_COUNT = 5;
        var DAYS_BEFORE_DELETION = 3;
        var PRIVATE_PROPERTY_NAME = "DeletionDate";
        var volunteer = await DataGenerator.SeedVolunteerWithPets(VolunteersWriteDbContext, SpeciesWriteDbContext, PET_COUNT);
        var pet1 = volunteer.AllOwnedPets[0];
        var pet2 = volunteer.AllOwnedPets[3];
        volunteer.SoftDeletePet(pet1);
        volunteer.SoftDeletePet(pet2);
        SetPrivatePropertyBase(pet1, PRIVATE_PROPERTY_NAME, DateTime.UtcNow.AddDays(-DAYS_BEFORE_DELETION + 1));
        SetPrivatePropertyBase(pet2, PRIVATE_PROPERTY_NAME, DateTime.UtcNow.AddDays(-DAYS_BEFORE_DELETION));
        
        // act
        await _sut.Process(DAYS_BEFORE_DELETION, CancellationToken.None);

        // assert
        volunteer.AllOwnedPets.Count.Should().Be(PET_COUNT - 1);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == pet1.Id).Should().NotBeNull();
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == pet2.Id).Should().BeNull();
    }
    
    [Fact]
    public async Task SoftDeletePet_success_should_delete_volunteer_with_expired_lifetime_and_all_its_pets()
    {
        // arrange
        var PET_COUNT = 3;
        var DAYS_BEFORE_DELETION = 3;
        var PRIVATE_PROPERTY_NAME = "DeletionDate";
        var volunteer1 = await DataGenerator.SeedVolunteerWithPets(VolunteersWriteDbContext, SpeciesWriteDbContext, PET_COUNT);
        var volunteer2 = await DataGenerator.SeedVolunteerWithPets(VolunteersWriteDbContext, SpeciesWriteDbContext, PET_COUNT);
        volunteer1.Delete();
        volunteer2.Delete();
        SetPrivatePropertyBase(volunteer1, PRIVATE_PROPERTY_NAME, DateTime.UtcNow.AddDays(-DAYS_BEFORE_DELETION));
        
        // act
        await _sut.Process(DAYS_BEFORE_DELETION, CancellationToken.None);

        // assert
        
        // soft deleted volunteer with expired lifetime should be soft deleted
        VolunteersReadDbContext.Volunteers.Count().Should().Be(1);
        VolunteersReadDbContext.Volunteers.FirstOrDefault(v => v.Id == volunteer1.Id).Should().BeNull();
        VolunteersReadDbContext.Pets.Count(p => p.OwnerId == volunteer1.Id).Should().Be(0);
        
        // soft deleted volunteer with not yet expired lifetime should not be deleted
        VolunteersReadDbContext.Volunteers.FirstOrDefault(v => v.Id == volunteer2.Id).Should().NotBeNull();
        VolunteersReadDbContext.Pets.Count(p => p.OwnerId == volunteer2.Id).Should().Be(PET_COUNT);

        // first volunteer pets should be actually removed from database and not just have nulls in some of their columns
        VolunteersReadDbContext.Pets.Count().Should().Be(PET_COUNT);


    }
    
    // reflection to get access to private setters
    static void SetPrivatePropertyBase<T>(T obj, string propertyName, object value)
    {
        PropertyInfo propertyInfo = typeof(T).BaseType.GetProperty(
            propertyName, 
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);
        
        if (propertyInfo == null)
        {
            throw new ArgumentException($"Свойство {propertyName} не найдено в иерархии типов");
        }
        
        propertyInfo.SetValue(obj, value);
    }
}