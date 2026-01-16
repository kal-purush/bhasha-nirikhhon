using Business.Features;
using Microsoft.AspNetCore.Mvc;

namespace API.Controllers
{
    [Produces("application/json")]
    [Route("api/Features")]
    public class FeaturesController : Controller
    {
        private readonly IFavoriteProducts _favoriteProducts;
        private readonly IReceiptManager _receiptManager;

        public FeaturesController(IFavoriteProducts favoriteProducts, IReceiptManager receiptManager)
        {
            _favoriteProducts = favoriteProducts;
            _receiptManager = receiptManager;
        }

        [HttpGet]
        public IActionResult GetFavoriteProducts()
        {
            var result = _favoriteProducts.GetFavoriteProducts();
            return Ok(result);
        }

        [HttpPost]
        public IActionResult UploadReceipt(string receipt)
        {
            var result = _receiptManager.Insert(receipt);
            return Ok(result);
        }
    }
}