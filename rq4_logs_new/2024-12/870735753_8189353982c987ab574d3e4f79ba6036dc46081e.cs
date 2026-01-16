using AutoMapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Store.G01.Apis.Error;
using Store.G01.Core.Dtos.Basket;
using Store.G01.Core.Entites;
using Store.G01.Core.RepostitoriesContract;
using Store.G01.Repository.Repositores;

namespace Store.G01.Apis.Controllers
{

	public class BasketController : BaseApiController
	{
		private readonly IBasketRepository _basketRepository;
		private readonly IMapper _mapper;

		public BasketController(IBasketRepository basketRepository,IMapper mapper)
		{
			_basketRepository = basketRepository;
			_mapper = mapper;
		}

		[HttpGet]
		public async Task<ActionResult<CustomerBasket>> GetBasket(string id)
		{
			if (id == null) return BadRequest(new ApiErrorResponce(400));
			var basket = await _basketRepository.GetBasketAsync(id);

			if (basket == null) return new CustomerBasket() { Id = id };

			return Ok(basket);

		}

		[HttpPost] //post : /api/basket

		public async Task<ActionResult<CustomerBasket>> CreatOrUpdateBasket(CustomerBasketDto model)
		{
			var basket= await _basketRepository.UpdateBasketAsync(_mapper.Map<CustomerBasket>(model));

			if (basket == null) return BadRequest(new ApiErrorResponce(400));

			return Ok(basket);
		}

		[HttpDelete]
		public async Task DeleteBasket(string id)
		{
			await _basketRepository.DeleteBasketAsync(id);
		}

	}
}