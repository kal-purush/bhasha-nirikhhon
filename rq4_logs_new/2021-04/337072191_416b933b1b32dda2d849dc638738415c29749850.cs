using BL.Services.QuestionTranslationServices;
using Common.DTOs.QuestionTranslationDTO;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace API.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class QuestionTranslationsController : ControllerBase
    {
        private readonly IQuestionTranslationService _service;

        public QuestionTranslationsController(IQuestionTranslationService service)
        {
            _service = service;
        }

        [HttpGet("language/{languageId}")]
        public ActionResult<IEnumerable<QuestionTranslationReadDto>> GetAllQuestionsByLanguage(int languageId)
        {
            var questions = _service.GetAllQuestionsByLanguage(languageId);

            if (questions != null)           
                return Ok(questions);

            return NotFound();
        }
    }
}