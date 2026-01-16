namespace DigitalFamilyCookbook.Handlers.Queries.Recipes;

public class QuickSearchRecipes
{
    public class Handler : IRequestHandler<Query, IReadOnlyCollection<RecipeApiModel>>
    {
        private readonly IRecipeRepository _recipeRepository;

        public Handler(IRecipeRepository recipeRepository)
        {
            _recipeRepository = recipeRepository;
        }

        public async Task<IReadOnlyCollection<RecipeApiModel>> Handle(Query request, CancellationToken cancellationToken)
        {
            try
            {
                if (request.Keywords.Trim() == string.Empty)
                {
                    throw new Exception("No search keywords provided");
                }

                var recipes = await Task.FromResult(_recipeRepository.QuickSearchRecipes(request.Keywords, request.MaxRecipes));

                return recipes
                    .Select(r => RecipeApiModel.FromDomainModel(r))
                    .OrderBy(r => r.Name)
                    .ToList();
            }
            catch (Exception ex)
            {
                return new List<RecipeApiModel>();
            }
        }
    }

    public class Query : IRequest<IReadOnlyCollection<RecipeApiModel>>
    {
        public string Keywords { get; set; } = string.Empty;

        public int MaxRecipes { get; set; } = 10;
    }
}