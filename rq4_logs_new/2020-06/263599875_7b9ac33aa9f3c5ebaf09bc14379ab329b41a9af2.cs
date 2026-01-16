using CM.DTOs;
using CM.Web.Areas.Magician.Models;
using CM.Web.Models;
using CM.Web.Providers.Contracts;
using System.Linq;

namespace CM.Web.Providers.ViewModelMappers
{
    public class CocktailViewMapper : ICocktailViewMapper
    {
        public CocktailViewModel CreateCocktailViewModel(CocktailDTO dto)
        {
            return new CocktailViewModel
            {
                Id = dto.Id,
                Name = dto.Name,
                Recipe = dto.Recipe,
                ImagePath = dto.ImagePath,
                Ingredients = dto.Ingredients
                                 .Select(i => CreateIngredientViewModel(i))
                                 .ToList(),
            };
        }

        private IngredientViewModel CreateIngredientViewModel(IngredientDTO dto)
        {
            return new IngredientViewModel
            {
                Id = dto.Id,
                Name = dto.Name,
                ImagePath = dto.ImagePath
            };
        }
    }
}