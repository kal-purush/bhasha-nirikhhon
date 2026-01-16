using AutoMapper;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using ModelLibrary.Dto;
using Services.ProductAPI.Data;
using Services.ProductAPI.Models;

namespace Services.ProductAPI.Controllers
{
	[Route("api/product")]
	[ApiController]
	[Authorize]
	public class ProductController : ControllerBase
	{
		private readonly AppDbContext _db;
		private IMapper _mapper;
		private ResponseDto _response;

		public ProductController(AppDbContext db, IMapper mapper)
		{
			_db = db;
			_mapper = mapper;
			_response = new();
		}

		[HttpGet]
		public ResponseDto Get()
		{
			try
			{
				var objList = _db.Products.ToList();
				_response.Result = _mapper.Map<IEnumerable<ProductDto>>(objList); ;
			}
			catch (Exception e)
			{
				_response.IsSuccess = false;
				_response.Message = e.Message;
			}
			return _response;
		}

		[HttpGet]
		[Route("{id:int}")]
		public ResponseDto Get(int id)
		{
			try
			{
				var obj = _db.Products.FirstOrDefault(u => u.ProductId == id);
				_response.Result = _mapper.Map<ProductDto>(obj); ;
			}
			catch (Exception e)
			{
				_response.IsSuccess = false;
				_response.Message = e.Message;
			}
			return _response;
		}

		[HttpPost]
		[Authorize(Roles = "ADMIN")]
		public ResponseDto Post([FromBody] ProductDto dto)
		{
			try
			{
				Product model = _mapper.Map<Product>(dto);
				_db.Products.Add(model);
				_db.SaveChanges();
			}
			catch (Exception e)
			{
				_response.IsSuccess = false;
				_response.Message = e.Message;
			}
			return _response;
		}

		[HttpPut]
		[Authorize(Roles = "ADMIN")]
		public ResponseDto Put([FromBody] ProductDto dto)
		{
			try
			{
				Product model = _mapper.Map<Product>(dto);
				_db.Products.Update(model);
				_db.SaveChanges();

				_response.Result = _mapper.Map<ProductDto>(_db.Products.First(u => u.ProductId == dto.ProductId));
			}
			catch (Exception e)
			{
				_response.IsSuccess = false;
				_response.Message = e.Message;
			}
			return _response;
		}

		[HttpDelete]
		[Route("{id:int}")]
		[Authorize(Roles = "ADMIN")]
		public ResponseDto Delete(int id)
		{
			try
			{
				Product obj = _db.Products.First(u => u.ProductId == id);
				_db.Products.Remove(obj);
				_db.SaveChanges();
			}
			catch (Exception e)
			{
				_response.IsSuccess = false;
				_response.Message = e.Message;
			}
			return _response;
		}

	}
}