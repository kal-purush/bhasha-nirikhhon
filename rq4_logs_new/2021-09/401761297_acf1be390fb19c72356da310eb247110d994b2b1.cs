using Disney.Application.Interfaces;
using Disney.Domain.Common;
using Disney.Domain.DTOs;
using Disney.Domain.Entities;
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
    public class CharactersController : ControllerBase
    {
        #region Objects and Constructor
        private readonly ICharactersService charactersService;
        public CharactersController(ICharactersService charactersService)
        {
            this.charactersService = charactersService;
        }
        #endregion

        [HttpGet]
        [Authorize]
        public async Task<ActionResult<IEnumerable<CharacterDTO>>> GetAllCharacters()
        {
            var response = charactersService.GetAllCharacters();

            return Ok(await response);
        }

        [HttpGet]
        [Authorize]
        public ActionResult<CharacterDTO> GetCharacterByName([FromQuery] string name)
        {
            var response = charactersService.GetCharacterByName(name);

            return Ok(response);
        }

        [HttpGet]
        [Authorize]
        public ActionResult<CharacterDTO> GetCharacterByAge([FromQuery] int age)
        {
            var response = charactersService.GetCharactersByAge(age);
            return Ok(response);
        }

        [HttpGet]
        [Authorize]
        public ActionResult<MovieSerieDTO> GetCharacterByMovieSerie([FromQuery] MovieSerieDTO movieSerieDTO)
        {
            var response = charactersService.GetCharacterByMovieSerie(movieSerieDTO);

            return Ok(response);
        }

        [HttpPost]
        [Authorize]
        public ActionResult<Result> CreateCharacter([FromBody] CharacterDTO characterDTO)
        {
            var response = charactersService.CreateCharacter(characterDTO);

            return response.HasErrors
                ? BadRequest(response.Messages)
                : Ok(response);
        }

        [HttpPut]
        [Authorize]

        public ActionResult<Result> UpdateCharacter([FromBody] CharacterDTO characterDTO)
        {
            var response = charactersService.UpdateCharacter(characterDTO);

            return response.HasErrors
                ? BadRequest(response.Messages)
                : Ok(response);
        }

        [HttpDelete]
        [Authorize]

        public ActionResult<Result> DeleteCharacter([FromRoute] CharacterDTO characterDTO)
        {
            var response = charactersService.DeleteCharacter(characterDTO);

            return response.HasErrors
                ? BadRequest(response.Messages)
                : Ok(response);
        }

    }
}