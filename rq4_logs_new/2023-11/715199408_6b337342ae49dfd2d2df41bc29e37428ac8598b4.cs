using Microsoft.AspNetCore.Mvc;
using NerdStore.Catalog.Application.Services;
using NerdStore.Catalog.Application.ViewModels;

namespace NerdStore.WebApp.MVC.Controllers.Admin;

public class ProductsAdminController : Controller
{
    private readonly IProductAppService _productAppService;

    public ProductsAdminController(IProductAppService productAppService)
    {
        _productAppService = productAppService;
    }

    [HttpGet("products-admin")]
    public async Task<IActionResult> Index()
    {
        return View(await _productAppService.GetAllAsync());
    }

    [HttpGet("new-product")]
    public async Task<IActionResult> NewProduct()
    {
        return View(await FillCategoriesAsync(new ProductViewModel()));
    }

    [HttpPost("new-product")]
    public async Task<IActionResult> NewProduct(ProductViewModel productViewModel)
    {
        if (!ModelState.IsValid) return View(await FillCategoriesAsync(productViewModel));

        await _productAppService.AddProductAsync(productViewModel);

        return RedirectToAction("Index");
    }

    [HttpGet("edit-product")]
    public async Task<IActionResult> UpdateProduct(Guid id)
    {
        return View(await FillCategoriesAsync(await _productAppService.GetByIdAsync(id)));
    }

    [HttpPost("edit-product")]
    public async Task<IActionResult> UpdateProduct(Guid id, ProductViewModel productViewModel)
    {
        var product = await _productAppService.GetByIdAsync(id);
        productViewModel.QuantityInStock = product.QuantityInStock;

        ModelState.Remove("QuantityInStock");
        if (!ModelState.IsValid) return View(await FillCategoriesAsync(productViewModel));

        await _productAppService.UpdateProductAsync(productViewModel);

        return RedirectToAction("Index");
    }

    [HttpGet("products-update-stock")]
    public async Task<IActionResult> UpdateStock(Guid id)
    {
        return View("Stock", await _productAppService.GetByIdAsync(id));
    }

    [HttpPost("products-update-stock")]
    public async Task<IActionResult> UpdateStock(Guid id, int quantity)
    {
        if (quantity > 0)
        {
            await _productAppService.ReplenishStockAsync(id, quantity);
        }
        else
        {
            await _productAppService.DebitStockAsync(id, quantity);
        }

        return View("Index", await _productAppService.GetAllAsync());
    }

    private async Task<ProductViewModel> FillCategoriesAsync(ProductViewModel product)
    {
        product.Categories = await _productAppService.GetCategoriesAsync();
        return product;
    }
}