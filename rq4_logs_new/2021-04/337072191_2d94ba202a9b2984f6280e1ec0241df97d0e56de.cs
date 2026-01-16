using BL.Services.RecommendationTranslationServices;
using Common.DTOs.RecommendationTranslationDTO;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace API.Controllers
{
    [Authorize]
    [Route("api/[controller]")]
    [ApiController]
    public class RecommendationTranslationsController : ControllerBase
    {
        private readonly IRecommendationTranslationService _service;

        public RecommendationTranslationsController(IRecommendationTranslationService service)
        {
            _service = service;
        }

        [HttpGet("{languageId}")]
        public ActionResult<IEnumerable<RecommendationTranslationReadDto>> GetAllRecommendationTranslationByLanguageId(int languageId)
        {
            var recommendations = _service.GetAllRecommendationTranslationByLanguageId(languageId);

            if (recommendations != null)
                return Ok(recommendations);

            return NotFound();
        }
    }
}