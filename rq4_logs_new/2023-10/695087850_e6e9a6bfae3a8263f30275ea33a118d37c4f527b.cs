using System.ComponentModel.DataAnnotations;
using FoodImportLabelGenerator.Data;
using FoodImportLabelGenerator.Models;
using FoodImportLabelGenerator.Repository;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore; //needed because this class derives from ControllerBase

namespace FoodImportLabelGenerator.Controllers;

//SRP - Any controller class should focus solely on handling the HTTP requests and responses and should be as 'thin' as possible

[ApiController]
[Route("[controller]")]
public class TranslationController : ControllerBase
{
    private readonly ILogger<TranslationController> _logger;
    private readonly IConfiguration _configuration;
    private readonly ITranslationRepository _translationRepository;

    public TranslationController(ILogger<TranslationController> logger, IConfiguration configuration, ITranslationRepository translationRepository)
    {
        _logger = logger;
        _configuration = configuration;
        _translationRepository = translationRepository;
    }

    [HttpGet("GetAllAsync"), Authorize(Roles = "User, Admin")]
    public async Task<ActionResult<IEnumerable<Translation>>> GetAllAsync()
    {
        var translations = _translationRepository.GetAll();
        if (translations == null)
        {
            return NotFound("There are no translations in the database.");
        }
        try
        {
            return Ok(translations);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error getting translations");
            return NotFound("Error getting translations.");
        }
    }
    
    
    [HttpGet("GetByIdAsync/{id}")]
    public async Task<ActionResult<Translation>> GetByIdAsync(int id)
    {
        var translation = _translationRepository.GetById(id);

        if (translation == null)
        {
            return NotFound($"Cannot find translation with id {id}.");
        }
        try
        {
            return Ok(translation);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error getting translation");
            return NotFound("Error getting translation.");
        }
    }

    [HttpGet("GetByEnglishWordAsync"), Authorize(Roles = "User, Admin")]
    public async Task<ActionResult<IEnumerable<Translation>>> GetByEnglishAsync([Required]string englishWord)
    {
        var translations = _translationRepository.GetByEnglishWord(englishWord);

        if (!translations.Any())
        {
            return NotFound($"Cannot find translation for {englishWord}.");
        }
        try
        {
            return Ok(translations);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error getting translations");
            return NotFound("Error getting translations.");
        }
    }

    [HttpGet("GetByHungarianWordAsync"), Authorize(Roles = "User, Admin")]
    public async Task<ActionResult<IEnumerable<Translation>>> GetByHungarianWordAsync([Required]string hungarianWord)
    {
        var translations = _translationRepository.GetByHungarianWord(hungarianWord);

        if (!translations.Any())
        {
            return NotFound($"Cannot find translation for {hungarianWord}.");
        }
        try
        {
            return Ok(translations);
        }
        catch (Exception e)
        {
            _logger.LogError(e, "Error getting translations");
            return NotFound("Error getting translations.");
        }
    }

    [HttpPost("AddAsync")]
    public async Task<ActionResult<IEnumerable<Translation>>> AddAsync([Required]string englishWord, [Required]string hungarianWord)
    {
        // Additional validation for mandatory parameters
        if (string.IsNullOrWhiteSpace(englishWord))
        {
            ModelState.AddModelError("englishWord", "English word field cannot be empty.");
        }
        
        if (string.IsNullOrWhiteSpace(hungarianWord))
        {
            ModelState.AddModelError("hungarianWord", "Hungarian word field cannot be empty.");
        }

        if (!ModelState.IsValid)
        {
            return BadRequest("Missing or invalid input data");
        }
        
        Translation newTranslation = new Translation()
        {
            English = englishWord,
            Hungarian = hungarianWord
        };
        _translationRepository.Add(newTranslation);
        return Ok("New translation added successfully.");
        //return Ok(new { Message = "New translation added successfully.", Translation = newTranslation });
    }

    [HttpPut("UpdateAsync/{id}")]
    public async Task<ActionResult<Translation>> UpdateAsync(int id, string englishWord, string hungarianWord)
    {
        Translation existingTranslation = _translationRepository.GetById(id);

        if (existingTranslation == null)
        {
            return NotFound($"It is not possible to update translation. There is no translation with id {id}.");
        }
        
        existingTranslation.English = string.IsNullOrEmpty(englishWord) ? existingTranslation.English : englishWord;
        existingTranslation.Hungarian = string.IsNullOrEmpty(hungarianWord) ? existingTranslation.Hungarian : hungarianWord;

        _translationRepository.Update(existingTranslation);

        return Ok(existingTranslation);
    }
    
    [HttpDelete("DeleteByIdAsync/{id}")]
    public async Task<ActionResult<Translation>> DeleteByIdAsync(int id)
    {
        Translation existingTranslation = _translationRepository.GetById(id);
        
        if (existingTranslation == null)
        {
            return NotFound($"It is not possible to delete translation. There is no translation with id {id}.");
        }

        _translationRepository.Delete(existingTranslation);
        return Ok($"Translation with id {id} has been deleted successfully.");
    }
}