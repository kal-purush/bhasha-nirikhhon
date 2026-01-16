using AskMe.Data.Entities;
using AskMe.Data.Models.Themes;
using AskMe.Services.Authenticators;
using AskMe.Services.Sets;
using AskMe.Services.Themes;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Security.Claims;

namespace AskMe.Controllers
{
    [Route("[controller]")]
    [ApiController]
    public class ThemeController : ControllerBase
    {
        private readonly ILogger<ThemeController> _logger;
        private readonly IThemeService _themeService;
        private readonly IAuthenticator _authenticator;
        public ThemeController(ILogger<ThemeController> logger, IThemeService themeService, IAuthenticator authenticator)
        {
            _logger = logger;
            _themeService = themeService;
            _authenticator = authenticator;
        }

        [HttpGet("set/{setId:int}")]
        [Authorize]
        public async Task<IActionResult> GetThemesBySet(int setId)
        {
            try
            {
                var userId = User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
                if (userId is null) return Unauthorized();
                if (!await _authenticator.AuthenticateUserToSet(userId, setId))
                    return Unauthorized("You are not authorized to access this set");

                var themes = await _themeService.GetAllBySet(setId);

                if (themes is null) return NotFound("No themes found for this set");

                return Ok(themes);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error while getting themes by set {ex.Message}");
                return StatusCode(500, "Getting themes went wrong.");
            }
        }

        [HttpGet("theme/{themeId:int}")]
        [Authorize]
        public async Task<IActionResult> GetThemeById(int themeId)
        {
            try
            {
                var userId = User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
                if (userId is null) return Unauthorized();
                if (!await _authenticator.AuthenticateUserToTheme(userId, themeId))
                    return Unauthorized("You are not authorized to access this set");

                var theme = await _themeService.GetById(themeId);

                if (theme is null) return NotFound("Theme not found");

                return Ok(theme);
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error while getting theme {ex.Message}");
                return StatusCode(500, "Getting theme went wrong.");
            }
        }

        [HttpPost]
        [Authorize]
        public async Task<IActionResult> CreateTheme([FromBody] ThemeDto theme)
        {
            try
            {
                var userId = User.FindFirst(ClaimTypes.NameIdentifier)?.Value;
                if (userId is null) return Unauthorized();
                if (!await _authenticator.AuthenticateUserToTheme(userId, theme.Id))
                    return Unauthorized("You are not authorized to access this set");

                var createdTheme = await _themeService.Create(theme);

                if (createdTheme is null) return BadRequest("Creating theme failed");

                return Ok(createdTheme);

            }
            catch (Exception ex)
            {
                _logger.LogError($"Error while creating theme {ex.Message}");
                return StatusCode(500, "Creating theme went wrong.");
            }
        }

        [HttpPatch]
        [Authorize]
        public async Task<IActionResult> UpdateTheme([FromBody] ThemeDto theme)
        {
            try
            {
                var userId = User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

                if (userId is null) return Unauthorized();
                if (!await _authenticator.AuthenticateUserToTheme(userId, theme.Id))
                    return Unauthorized("You are not authorized to access this set");

                var isSuccess = await _themeService.Update(theme);

                if (!isSuccess) return BadRequest("Updating theme failed");

                return Ok("Theme updated successfully");
            }
            catch (Exception ex)
            {
                _logger.LogError($"Error while updating theme {ex.Message}");
                return StatusCode(500, "Updating theme went wrong.");
            }
        }

        [HttpDelete("{themeId:int}")]
        [Authorize]
        public async Task<IActionResult> DeleteTheme(int themeId)
        {
            try
            {
                var userId = User.FindFirst(ClaimTypes.NameIdentifier)?.Value;

                if (userId is null) return Unauthorized();
                if (!await _authenticator.AuthenticateUserToTheme(userId, themeId))
                    return Unauthorized("You are not authorized to access this set");

                var isSuccess = await _themeService.Delete(themeId);

                if (!isSuccess) return BadRequest("Deleting theme failed");

                return Ok("Theme deleted successfully");

            }
            catch (Exception ex)
            {
                _logger.LogError($"Error while deleting theme {ex.Message}");
                return StatusCode(500, "Deleting theme went wrong.");
            }
        }
    }
}