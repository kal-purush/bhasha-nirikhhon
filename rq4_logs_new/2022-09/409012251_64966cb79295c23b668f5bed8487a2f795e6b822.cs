using DigitalFamilyCookbook.Handlers.Queries.Recipes;

namespace DigitalFamilyCookbook.Tests.Handlers.Queries.Recipes;

public class GetMostFavoritedRecipesTests
{
    private readonly Mock<IRecipeRepository> _recipeRepository;
    private readonly Mock<IFileService> _fileService;

    public GetMostFavoritedRecipesTests()
    {
        _recipeRepository = new Mock<IRecipeRepository>();
        _fileService = new Mock<IFileService>();
    }

    [Fact]
    public async Task ItRetrievesTheMostRecentRecipes()
    {
        var recipes = MockRecipe.GenerateDomainModelList(10);

        _recipeRepository
            .Setup(r => r.GetMostFavoritedRecipes(10))
            .Returns(recipes);

        var query = new GetMostFavoritedRecipes.Query
        {
            Count = 10,
            IncludeImages = false,
        };

        var handler = new GetMostFavoritedRecipes.Handler(_recipeRepository.Object, _fileService.Object);

        var result = await handler.Handle(query, new CancellationToken());

        Assert.Equal(10, result.Count);
    }
}