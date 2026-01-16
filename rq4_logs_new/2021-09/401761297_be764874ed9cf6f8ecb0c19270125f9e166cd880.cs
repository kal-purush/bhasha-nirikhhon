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
    public class GenresController : ControllerBase
    {
        #region Object and Constructor
        private readonly IGenresServices genreServices;
        public GenresController(IGenresServices genreServices)
        {
            this.genreServices = genreServices;
        }
        #endregion

        [HttpGet]
        [Authorize]
        public async Task<ActionResult<IEnumerable<GenreDTO>>> GetAllGenres()
        {
            var response = genreServices.GetAllGenres();

            return Ok(await response);
        }

        [HttpPost]
        [Authorize]
        public ActionResult<Result> CreateGenre([FromBody] GenreDTO genreDTO)
        {
            var response = genreServices.CreateGenre(genreDTO);

            return response.HasErrors
                ? BadRequest(response.Messages)
                : Ok(response);
        }

        [HttpPut]   
        [Authorize]
        public ActionResult<Result> UpdateGenre([FromBody] GenreDTO genreDTO)
        {
            var response = genreServices.UpdateGenre(genreDTO);

            return response.HasErrors
                ? BadRequest(response.Messages)
                : Ok(response);
        }

        [HttpDelete]
        [Authorize]
        public ActionResult<Result> DeleteGenre([FromRoute] GenreDTO genreDTO)
        {
            var response = genreServices.DeleteGenre(genreDTO);

            return response.HasErrors
                ? BadRequest(response.Messages)
                : Ok(response);
        }

    }
}