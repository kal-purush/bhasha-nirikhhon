using Microsoft.AspNetCore.Components;
using ShopOnline.Models.Dtos;
using ShopOnline.Web.Services.Contracts;

namespace ShopOnline.Web.Pages
{
    public class ShoppingCartBase:ComponentBase
    {
        [Inject]
        public IShoppingCartService _ShoppingCartService { get; set; }

        public IEnumerable<CartItemDto> ShoppingCartItems { get; set; }

        public string ErrorMessage { get; set; }

        // implement  code  for assigning the relevent items that return from the server to a shoppingCartItems property when the razor component first randerd
        protected override async Task OnInitializedAsync()
        {
            try
            {
                ShoppingCartItems = await _ShoppingCartService.GetItems(HardCodded.UserId);
            }
            catch (Exception ex)
            {

                ErrorMessage = ex.Message;
            }
        }

    }
}