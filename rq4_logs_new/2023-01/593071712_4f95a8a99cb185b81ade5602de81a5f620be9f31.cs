using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using NewsBook.Data;
using NewsBook.ModelDTO;
using NewsBook.Models.Paging;
using NewsBook.Repository;
using System.ComponentModel;
using System.Linq;
using System.Security.Claims;

namespace NewsBook.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class NewsController : ControllerBase
    {
        private readonly INewsRepository _newsRepository;
        private readonly IFavouriteNewsRespository _favouriteNews;
        public NewsController(
            INewsRepository newsRepository, 
            IFavouriteNewsRespository favouriteNews
        )
        {
            _newsRepository = newsRepository;
            _favouriteNews = favouriteNews;
        }

        //Get All News
        [HttpGet]
        
        public async Task<IActionResult> Get([FromQuery] NewsParameters newsParameters)
        {
            var _news = await _newsRepository.GetAll(newsParameters);
            return Ok(_news);
        }

        [HttpGet]
        [Route("{Id}")]
        public async Task<IActionResult> Get(Guid Id)
        {
            var getById = await _newsRepository.GetById(Id);
            if (getById != null)
            {
                return Ok(getById);
            }
            else
            {
                return NotFound(Id);
            }
        }

        //Post News
        [HttpPost]
        public async Task<IActionResult> Post(NewsWriteDTO newsDTO)
        {
            var user = await _newsRepository.Insert(newsDTO.Tittle, newsDTO.Description);
            return Ok(user);
        }
        [HttpPut]
        [Route("{Id:guid}")]
        //[Route("search/{title}")]
        public async Task<IActionResult> Put(Guid Id, [FromBody] NewsWriteDTO NewsDTO)
        {

            var news = await _newsRepository.GetById(Id);
            if (news == null)
            {
                return BadRequest(Id);
            }

            news.Title = NewsDTO.Tittle;
            news.Description = NewsDTO.Description;

            news = await _newsRepository.Update(news);
            return Ok(news);
        }
        [HttpDelete]
        [Route("{Id}")]
        public async Task<IActionResult> Delete(Guid Id)
        {
            var news = await _newsRepository.GetById(Id);
            if (news == null)
            {
                return BadRequest(Id);
            }

            return Ok(await _newsRepository.Delete(Id));
        }

        [HttpPost]
        [Route("MarkFavourite")]
        public async Task<IActionResult> MarkFavourite([FromBody] FavouriteNewsWriteDTO FavouriteNewsDTO)
        {
            var favouriteNews = await _favouriteNews.Insert(
                FavouriteNewsDTO.NewsId, 
                FavouriteNewsDTO.UserId, 
                FavouriteNewsDTO.IsFavourite
            );
            return Ok(favouriteNews);
        }

        [HttpGet]
        [Route("Favourite")]
        public async Task<IActionResult> GetFavouriteNews([FromQuery] Guid UserId)
        {
            var news = await _newsRepository.GetFavouriteNews(UserId);
            return Ok(news);
        }
    }
}