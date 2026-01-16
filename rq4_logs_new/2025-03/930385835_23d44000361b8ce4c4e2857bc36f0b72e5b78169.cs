using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using PetFamily.Application.Abstractions;
using PetFamily.Application.Species.Commands.AddBreedToSpecies;
using PetFamily.IntegrationTests.General;
using PetFamily.IntegrationTests.SpeciesNS.Heritage;

namespace PetFamily.IntegrationTests.SpeciesNS.HandlerTests;

public class AddBreedToSpeciesHandlerTests : SpeciesTestsBase
{
    private readonly ICommandHandler<Guid, AddBreedToSpeciesCommand> _sut;

    public AddBreedToSpeciesHandlerTests(SpeciesTestsWebFactory factory) : base(factory)
    {
        _sut = Scope.ServiceProvider.GetRequiredService<ICommandHandler<Guid, AddBreedToSpeciesCommand>>();
    }

    [Fact]
    public async Task CreateSpecies_success_should_return_guid_of_created_species()
    {
        // arrange
        var BREED_NAME = "Test Breed";
        var species = await DataGenerator.SeedSpecies(WriteDbContext);
        var existedBred = await DataGenerator.SeedBreed(WriteDbContext, species.Id);
        var command = new AddBreedToSpeciesCommand(species.Id, BREED_NAME);

        // act
        var result = await _sut.HandleAsync(command, CancellationToken.None);

        // assert
        result.IsSuccess.Should().BeTrue();
        result.Value.Should().NotBeEmpty();
        
        var updatedSpecies = await WriteDbContext.Species.FirstOrDefaultAsync(v => v.Id == species.Id);
        species.Breeds.Count.Should().Be(2); // both existed and added breeds are now in database
        species.Breeds.FirstOrDefault(breed => breed.Name.Value == BREED_NAME).Should().NotBeNull();
    }
}