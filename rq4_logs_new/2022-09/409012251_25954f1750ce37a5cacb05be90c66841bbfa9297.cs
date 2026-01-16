namespace DigitalFamilyCookbook.Handlers.Queries.Recipes;

public class GetMostFavoritedRecipes
{
    public class Handler : IRequestHandler<Query, IReadOnlyCollection<RecipeApiModel>>
    {
        private readonly IRecipeRepository _recipeRepository;
        private readonly IFileService _fileService;

        public Handler(
            IRecipeRepository recipeRepository,
            IFileService fileService
        )
        {
            _recipeRepository = recipeRepository;
            _fileService = fileService;
        }

        public async Task<IReadOnlyCollection<RecipeApiModel>> Handle(Query request, CancellationToken cancellationToken)
        {
            var data = await Task.FromResult(_recipeRepository.GetMostFavoritedRecipes(request.Count));

            var recipes = data
                .Select(r => RecipeApiModel.FromDomainModel(r))
                .ToList();

            if (!request.IncludeImages)
            {
                return recipes;
            }

            foreach (var recipe in recipes)
            {
                if (recipe.ImageUrl?.Length > 0)
                {
                    try
                    {
                        // don't fail the whole process if the image can't be found
                        recipe.ImageData = _fileService.GetRecipeImage(recipe.ImageUrl);
                    }
                    catch { }
                }

                if (recipe.ImageUrlLarge?.Length > 0)
                {
                    try
                    {
                        // don't fail the whole process if the image can't be found
                        recipe.LargeImageData = _fileService.GetRecipeImage(recipe.ImageUrlLarge);
                    }
                    catch { }
                }
            }

            return recipes;
        }
    }

    public class Query : IRequest<IReadOnlyCollection<RecipeApiModel>>
    {
        public int Count { get; set; } = 8;

        public bool IncludeImages { get; set; }
    }
}