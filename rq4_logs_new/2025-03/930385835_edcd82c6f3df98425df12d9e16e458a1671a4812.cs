using System.Windows.Input;
using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using PetFamily.Application.Abstractions;
using PetFamily.Application.Volunteers.Commands.ChangePetPosition;
using PetFamily.Application.Volunteers.Commands.DeletePet;
using PetFamily.IntegrationTests.General;
using PetFamily.IntegrationTests.Volunteers.Heritage;

namespace PetFamily.IntegrationTests.Volunteers.HandlersTests;

public class ChangePetPositionHandlerTests : VolunteerTestsBase
{
    private readonly ICommandHandler<Guid, ChangePetPositionCommand> _sut;
    public ChangePetPositionHandlerTests(VolunteerTestsWebFactory factory) : base(factory)
    {
        _sut = Scope.ServiceProvider.GetRequiredService<ICommandHandler<Guid, ChangePetPositionCommand>>();
    }
    
    [Fact]
    public async Task ChangePosition_failure_new_position_is_below_1()
    {
        // arrange
        var PET_COUNT = 5;
        var NEW_PET_POSITION = 0;
        var OLD_POSITION = 1;
        var volunteer = await DataGenerator.SeedVolunteerWithPets(WriteDbContext, PET_COUNT);
        var pet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == OLD_POSITION);
        var command = new ChangePetPositionCommand(volunteer.Id, pet.Id, NEW_PET_POSITION);
        
        // act
        var result = await _sut.HandleAsync(command, CancellationToken.None);

        // assert
        result.IsFailure.Should().BeTrue();
    }
    
    [Fact]
    public async Task ChangePosition_success_new_position_is_higher_than_amount_of_pets()
    {
        // arrange
        var PET_COUNT = 5;
        var NEW_PET_POSITION = 100;
        var OLD_POSITION = 1;
        var volunteer = await DataGenerator.SeedVolunteerWithPets(WriteDbContext, PET_COUNT);
        var pet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == OLD_POSITION);
        var firstPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 1);
        var secondPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 2);
        var thirdPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 3);
        var fourthPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 4);
        var fifthPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 5);
        var command = new ChangePetPositionCommand(volunteer.Id, pet.Id, NEW_PET_POSITION);
        
        // act
        var result = await _sut.HandleAsync(command, CancellationToken.None);

        // assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().NotBeEmpty();

        volunteer = await WriteDbContext.Volunteers.FirstOrDefaultAsync(v => v.Id == volunteer.Id);
        volunteer!.AllOwnedPets.FirstOrDefault(p => p.Id == result.Value)
            .Position.Value.Should().Be(PET_COUNT);

        // position of each pet are adjusted
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == firstPet.Id).Position.Value.Should().Be(5);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == secondPet.Id).Position.Value.Should().Be(1);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == thirdPet.Id).Position.Value.Should().Be(2);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == fourthPet.Id).Position.Value.Should().Be(3);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == fifthPet.Id).Position.Value.Should().Be(4);
    }
    
    [Fact]
    public async Task ChangePosition_success_new_position_is_same_as_former()
    {
        // arrange
        var PET_COUNT = 5;
        var NEW_PET_POSITION = 3;
        var OLD_POSITION = 3;
        var volunteer = await DataGenerator.SeedVolunteerWithPets(WriteDbContext, PET_COUNT);
        var pet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == OLD_POSITION);
        var firstPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 1);
        var secondPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 2);
        var thirdPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 3);
        var fourthPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 4);
        var fifthPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 5);
        var command = new ChangePetPositionCommand(volunteer.Id, pet.Id, NEW_PET_POSITION);
        
        // act
        var result = await _sut.HandleAsync(command, CancellationToken.None);

        // assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().NotBeEmpty();

        volunteer = await WriteDbContext.Volunteers.FirstOrDefaultAsync(v => v.Id == volunteer.Id);
        volunteer!.AllOwnedPets.FirstOrDefault(p => p.Id == result.Value)
            .Position.Value.Should().Be(NEW_PET_POSITION);

        // position of each pet are adjusted
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == firstPet.Id).Position.Value.Should().Be(1);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == secondPet.Id).Position.Value.Should().Be(2);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == thirdPet.Id).Position.Value.Should().Be(3);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == fourthPet.Id).Position.Value.Should().Be(4);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == fifthPet.Id).Position.Value.Should().Be(5);
    }
    
    [Fact]
    public async Task ChangePosition_success_from_lower_non_border_to_higher_non_border()
    {
        // arrange
        var PET_COUNT = 5;
        var NEW_PET_POSITION = 4;
        var OLD_POSITION = 2;
        var volunteer = await DataGenerator.SeedVolunteerWithPets(WriteDbContext, PET_COUNT);
        var pet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == OLD_POSITION);
        var firstPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 1);
        var secondPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 2);
        var thirdPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 3);
        var fourthPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 4);
        var fifthPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 5);
        var command = new ChangePetPositionCommand(volunteer.Id, pet.Id, NEW_PET_POSITION);
        
        // act
        var result = await _sut.HandleAsync(command, CancellationToken.None);

        // assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().NotBeEmpty();

        volunteer = await WriteDbContext.Volunteers.FirstOrDefaultAsync(v => v.Id == volunteer.Id);
        volunteer!.AllOwnedPets.FirstOrDefault(p => p.Id == result.Value)
            .Position.Value.Should().Be(NEW_PET_POSITION);

        // position of each pet are adjusted
        // position of each pet are adjusted
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == firstPet.Id).Position.Value.Should().Be(1);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == secondPet.Id).Position.Value.Should().Be(4);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == thirdPet.Id).Position.Value.Should().Be(2);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == fourthPet.Id).Position.Value.Should().Be(3);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == fifthPet.Id).Position.Value.Should().Be(5);
    }
    
    [Fact]
    public async Task ChangePosition_success_from_higher_non_border_to_lower_non_border()
    {
        // arrange
        var PET_COUNT = 5;
        var NEW_PET_POSITION = 2;
        var OLD_POSITION = 4;
        var volunteer = await DataGenerator.SeedVolunteerWithPets(WriteDbContext, PET_COUNT);
        var pet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == OLD_POSITION);
        var firstPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 1);
        var secondPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 2);
        var thirdPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 3);
        var fourthPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 4);
        var fifthPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 5);
        var command = new ChangePetPositionCommand(volunteer.Id, pet.Id, NEW_PET_POSITION);
        
        // act
        var result = await _sut.HandleAsync(command, CancellationToken.None);

        // assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().NotBeEmpty();

        volunteer = await WriteDbContext.Volunteers.FirstOrDefaultAsync(v => v.Id == volunteer.Id);
        volunteer!.AllOwnedPets.FirstOrDefault(p => p.Id == result.Value)
            .Position.Value.Should().Be(NEW_PET_POSITION);

        // position of each pet are adjusted
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == firstPet.Id).Position.Value.Should().Be(1);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == secondPet.Id).Position.Value.Should().Be(3);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == thirdPet.Id).Position.Value.Should().Be(4);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == fourthPet.Id).Position.Value.Should().Be(2);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == fifthPet.Id).Position.Value.Should().Be(5);
    }
    
    [Fact]
    public async Task ChangePosition_success_from_highest_position_to_lowest()
    {
        // arrange
        var PET_COUNT = 5;
        var NEW_PET_POSITION = 1;
        var OLD_POSITION = 5;
        var volunteer = await DataGenerator.SeedVolunteerWithPets(WriteDbContext, PET_COUNT);
        var pet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == OLD_POSITION);
        var firstPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 1);
        var secondPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 2);
        var thirdPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 3);
        var fourthPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 4);
        var fifthPet = volunteer.AllOwnedPets.FirstOrDefault(p => p.Position.Value == 5);
        var command = new ChangePetPositionCommand(volunteer.Id, pet.Id, NEW_PET_POSITION);
        
        // act
        var result = await _sut.HandleAsync(command, CancellationToken.None);

        // assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().NotBeEmpty();

        volunteer = await WriteDbContext.Volunteers.FirstOrDefaultAsync(v => v.Id == volunteer.Id);
        volunteer!.AllOwnedPets.FirstOrDefault(p => p.Id == result.Value)
            .Position.Value.Should().Be(NEW_PET_POSITION);

        // position of each pet are adjusted
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == firstPet.Id).Position.Value.Should().Be(2);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == secondPet.Id).Position.Value.Should().Be(3);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == thirdPet.Id).Position.Value.Should().Be(4);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == fourthPet.Id).Position.Value.Should().Be(5);
        volunteer.AllOwnedPets.FirstOrDefault(p => p.Id == fifthPet.Id).Position.Value.Should().Be(1);
    }
}