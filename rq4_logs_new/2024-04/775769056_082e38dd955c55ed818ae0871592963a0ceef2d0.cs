using LagDaemon.YAMUD.Model.Characters;
using LagDaemon.YAMUD.WebAPI.Services.CharacterServices;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

// For more information on enabling Web API for empty projects, visit https://go.microsoft.com/fwlink/?LinkID=397860

namespace LagDaemon.YAMUD.WebAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class CharacterController : ControllerBase
    {
        private ICharacterGenerationService _characterGenerationService;
        private ICharacterService _characterService;

        public CharacterController(
            ICharacterGenerationService characterGenerationService,
            ICharacterService characterService
            )
        {
            _characterGenerationService = characterGenerationService;
            _characterService = characterService; 
        }

        [Authorize]
        [HttpGet("AllCharacters")]
        public async Task<IActionResult> AllCharacters()
        {
            var characters = await _characterService.GetAll();
            return Ok(characters);
        }

        [Authorize]
        [HttpGet("AllCharactersForCurrentUser")]
        public async Task<IActionResult> AllCharactersForCurrentUser()
        {
            var characters = await _characterService.GetAllForCurrentUser();
            return Ok(characters);
        }

        [Authorize]
        [HttpGet("GenerateCharacter")]
        public async Task<IActionResult> GenerateCharacter()
        {
            var character = await _characterGenerationService.GenerateCharacter();
            return Ok(character);
        }

        [Authorize]
        [HttpPost("CreateCharacter")]
        public async Task<IActionResult> CreateCharacter([FromBody] Character character)
        {
            var result = await _characterService.CreateCharacter(character);
            return Ok(result);
        }

        [Authorize]
        [HttpPost("DeleteCharacter")]
        public async Task<IActionResult> DeleteCharacter([FromBody] Guid id)
        {
            await _characterService.DeleteCharacter(id);
            return Ok();
        }

        [Authorize]
        [HttpPost("UpdateOrCreateCharacter")]
        public async Task<IActionResult> UpdateOrCreateCharacter([FromBody] Character character)
        {
            var result = await _characterService.UpdateOrCreateCharacter(character);
            return Ok(result);
        }

    }
}