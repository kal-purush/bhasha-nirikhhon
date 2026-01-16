using Microsoft.AspNetCore.Mvc;
using Milionaires.Database.Entities;
using Milionaires.Models;
using Milionaires.Service;

namespace Milionaires.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class GameController : ControllerBase
    {
        private readonly IGameService _service;
        public GameController(IGameService service)
        {
            _service = service;
        }

        [HttpGet("questions")]
        public IActionResult GetQuestions()
        {
            var _questions = _service.GetAllQuestions();

            if (_questions == null)
            {
                return NotFound(new { message = "Nie masz jeszcze stworzonych pytań!", data = _questions });
            }

            return Ok(_questions);
        }
        [HttpGet("scores")]
        public IActionResult GetScores([FromQuery] ScoreDto scoreDto)
        {
            ScoreQuery scores = _service.GetAllScores(scoreDto);

            if(scores == null)
            {
                return NotFound(new { message = "Nie udało połączyć się z bazą danych!" });
            }
            return Ok(scores);
        }

        [HttpPost]
        public IActionResult SaveScore([FromBody] Score score)
        {
            try
            {
                Score record = _service.CreateRecord(score);
                return Ok("Wynik został zapisany!");
            }
            catch (InvalidOperationException ex)
            {
                return BadRequest(ex.Message);
            }
            catch (Exception)
            {
                return StatusCode(500, new { error = "Wystąpił błąd serwera! Spróbuj ponownie później lub skontaktuj się z administratorem." });
            }
        }
    }
}