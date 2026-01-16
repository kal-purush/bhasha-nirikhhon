using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Movies.API.Dtos;
using Movies.Core.Interfaces;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace Movies.API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class GenresController : ControllerBase
    {
        private readonly IGenreService _genreService;
        public GenresController(IGenreService genreService)
        {
            _genreService = genreService;
        }
        // GET: api/<MoviesController>
        [HttpGet]
        [ProducesResponseType(typeof(List<GenreDTO>), StatusCodes.Status200OK)]
        public async Task<IActionResult> GetGenres()
        {
            var movies = await _genreService.GetGenresAsync();
            return Ok(movies.Select(GenreDTO.FromEntity).ToList());
        }
    }
}