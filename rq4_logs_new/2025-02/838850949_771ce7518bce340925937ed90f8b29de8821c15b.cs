using Microsoft.AspNetCore.Mvc;
using ModelLibrary.Dto;
using Newtonsoft.Json;
using Web.Service;
using Web.Service.IService;

namespace Web.Controllers
{
	public class ProductController : Controller
	{
		private readonly IProductService _service;

		public ProductController(IProductService service)
		{
			_service = service;
		}

		public async Task<IActionResult> ProductIndex()
		{
			List<ProductDto>? list = new();
			ResponseDto? response = await _service.GetAllProductsAsync();
			if (response != null && response.IsSuccess)
			{
				list = JsonConvert.DeserializeObject<List<ProductDto>>(Convert.ToString(response.Result));
			}
			else
			{
				TempData["error"] = response?.Message;
			}
			return View(list);
		}

		public IActionResult ProductCreate()
		{
			return View();
		}

		[HttpPost]
		public async Task<IActionResult> ProductCreate(ProductDto dto)
		{
			if (ModelState.IsValid)
			{
				ResponseDto? response = await _service.CreateProductAsync(dto);
				if (response != null && response.IsSuccess)
				{
					TempData["success"] = "Product created successfully!";
					return RedirectToAction(nameof(ProductIndex));
				}
				else
				{
					TempData["error"] = response?.Message;
				}
			}
			return View();
		}

		public async Task<IActionResult> ProductDelete(int id)
		{
			ResponseDto? response = await _service.GetProductByIdAsync(id);
			if (response == null || !response.IsSuccess)
			{
				TempData["error"] = response?.Message;
				return RedirectToAction(nameof(ProductIndex));
			}
			
			ProductDto? dto = JsonConvert.DeserializeObject<ProductDto>(Convert.ToString(response.Result));
			ResponseDto? responseCreate = await _service.CreateProductAsync(dto);
			if (responseCreate == null || !responseCreate.IsSuccess)
			{
				TempData["error"] = responseCreate?.Message;
				return RedirectToAction(nameof(ProductIndex));
			}
			
			TempData["success"] = "Product created successfully!";
			return RedirectToAction(nameof(ProductIndex));
		}
	}
}