using Disney.Application.Interfaces;
using Disney.Domain.Common;
using Disney.Domain.DTOs;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Disney.API.Controllers
{
    [Authorize(AuthenticationSchemes = JwtBearerDefaults.AuthenticationScheme)]
    [Route("api/[controller]")]
    [ApiController]
    public class MoviesSeriesController : ControllerBase
    {
        #region Object and Constructor
        private readonly IMoviesSeriesServices moviesSeriesServices;
        public MoviesSeriesController(IMoviesSeriesServices moviesSeriesServices)
        {
            this.moviesSeriesServices = moviesSeriesServices;
        } 
        #endregion

        [HttpGet]
        [Authorize]
        public async Task<ActionResult<IEnumerable<MovieSerieDTO>>> GetAllMoviesSeries()
        {
            var response = moviesSeriesServices.GetAllMoviesSeries();

            return Ok(await response);
        }

        [HttpGet]
        [Authorize]
        public ActionResult<Result> GetMovieSerieByName([FromQuery] string name)
        {
            var response = moviesSeriesServices.GetMovieSerieByName(name);

            return Ok(response);
        }

        [HttpGet]
        [Authorize]
        public ActionResult<IEnumerable<GenreDTO>> GetMovieSerieByGenre([FromQuery] GenreDTO genreDTO)
        {
            var response = moviesSeriesServices.GetMovieSerieByGenre(genreDTO);

            return Ok(response);
        }

        [HttpPost]
        [Authorize]
        public ActionResult<Result> CreateMovieSerie([FromBody] MovieSerieDTO movieSerieDTO)
        {
            var response = moviesSeriesServices.CreateMovieSerie(movieSerieDTO);

            return response.HasErrors
                ? BadRequest(response.Messages)
                : Ok(response);
        }

        [HttpPut]
        [Authorize]
        public ActionResult<Result> UpdateMovieSerie([FromBody] MovieSerieDTO movieSerieDTO)
        {
            var response = moviesSeriesServices.UpdateMovieSerie(movieSerieDTO);
            return response.HasErrors
                ? BadRequest(response.Messages)
                : Ok(response);
        }

        [HttpDelete]
        [Authorize]
        public ActionResult<Result> DeleteMovieSerie([FromRoute] MovieSerieDTO movieSerieDTO)
        {
            var response = moviesSeriesServices.DeleteMovieSerie(movieSerieDTO);

            return response.HasErrors
                ? BadRequest(response.Messages)
                : Ok(response);
        }
    }
}