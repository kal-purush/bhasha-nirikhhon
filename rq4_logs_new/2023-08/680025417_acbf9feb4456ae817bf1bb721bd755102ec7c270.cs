using Microsoft.AspNetCore.Components;
using ShopOnline.Models.Dtos;
using ShopOnline.Web.Services.Contracts;

namespace ShopOnline.Web.Shared
{
    public class ProductCategoriesNavMenuBase : ComponentBase
    {
        [Inject]
        public IProductService _ProductService { get; set; }

        public IEnumerable<ProductCategoryDto> ProductCategoryDtos { get; set; }

        public string ErrorMessage { get; set; }

        protected async override Task OnInitializedAsync()
        {
            try
            {
                ProductCategoryDtos = await _ProductService.GetProductCategories();


            }
            catch (Exception ex)
            {

                ErrorMessage = ex.Message;
            }
        }

    }
}