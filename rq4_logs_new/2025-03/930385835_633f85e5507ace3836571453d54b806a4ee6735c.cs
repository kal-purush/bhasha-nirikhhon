using FluentAssertions;
using Microsoft.Extensions.DependencyInjection;
using PetFamily.Application.Abstractions;
using PetFamily.Application.Breeds.Queries.GetBreedsBySpeciesId;
using PetFamily.Application.Dto.Breed;
using PetFamily.Application.Dto.Species;
using PetFamily.Application.Volunteers.Queries.GetVolunteerById;
using PetFamily.Domain.PetContext.ValueObjects.VolunteerVO;
using PetFamily.Domain.Shared.SharedVO;
using PetFamily.IntegrationTests.General;
using PetFamily.IntegrationTests.SpeciesNS.Heritage;
using PetFamily.IntegrationTests.Volunteers.Heritage;

namespace PetFamily.IntegrationTests.SpeciesNS.HandlerTests;

public class GetBreedsBySpeciesIdHandlerTests : SpeciesTestsBase
{
    private readonly IQueryHandler<List<BreedDto>, GetBreedsBySpeciesIdQuery> _sut;

    public GetBreedsBySpeciesIdHandlerTests(SpeciesTestsWebFactory factory) : base(factory)
    {
        _sut = Scope.ServiceProvider.GetRequiredService<IQueryHandler<List<BreedDto>, GetBreedsBySpeciesIdQuery>>();
    }
    
    [Fact]
    public async Task GetBreedsBySpeciesId_returns_all_breeds_for_speciesId()
    {
        // arrange
        var REQUIRED_PET_COUNT = 3;
        var species1 = await DataGenerator.SeedSpecies(WriteDbContext);
        var breed1 = await DataGenerator.SeedBreed(WriteDbContext, species1.Id);
        var breed2 = await DataGenerator.SeedBreed(WriteDbContext, species1.Id);
        var breed3 = await DataGenerator.SeedBreed(WriteDbContext, species1.Id);
        
        var species2 = await DataGenerator.SeedSpecies(WriteDbContext);
        var breed4 = await DataGenerator.SeedBreed(WriteDbContext, species2.Id);
        var breed5 = await DataGenerator.SeedBreed(WriteDbContext, species2.Id);
        var query = new GetBreedsBySpeciesIdQuery(species1.Id);

        // act
        var result = await _sut.HandleAsync(query, CancellationToken.None);
        
        // assert
        result.Should().NotBeNull();
        result.Count.Should().Be(REQUIRED_PET_COUNT);
    }
    
    
    [Fact]
    public async Task GetBreedsBySpeciesId_returns_empty_list_if_there_are_no_breeds_for_speciesId()
    {
        // arrange
        var species1 = await DataGenerator.SeedSpecies(WriteDbContext);
        var breed1 = await DataGenerator.SeedBreed(WriteDbContext, species1.Id);
        var breed2 = await DataGenerator.SeedBreed(WriteDbContext, species1.Id);
        var breed3 = await DataGenerator.SeedBreed(WriteDbContext, species1.Id);
        
        var species2 = await DataGenerator.SeedSpecies(WriteDbContext);
        var query = new GetBreedsBySpeciesIdQuery(species2.Id);

        // act
        var result = await _sut.HandleAsync(query, CancellationToken.None);
        
        // assert
        result.Should().BeEmpty();
    }
    
    [Fact]
    public async Task GetBreedsBySpeciesId_returns_empty_list_if_there_are_no_such_speciesId()
    {
        // arrange
        var species1 = await DataGenerator.SeedSpecies(WriteDbContext);
        var breed1 = await DataGenerator.SeedBreed(WriteDbContext, species1.Id);
        var breed2 = await DataGenerator.SeedBreed(WriteDbContext, species1.Id);
        var breed3 = await DataGenerator.SeedBreed(WriteDbContext, species1.Id);
        
        var query = new GetBreedsBySpeciesIdQuery(Guid.NewGuid());

        // act
        var result = await _sut.HandleAsync(query, CancellationToken.None);
        
        // assert
        result.Should().BeEmpty();
    }
}