using AutoMapper;
using DiaryApp.Core.Models;
using DiaryApp.Data.DTO;
using DiaryApp.Data.ServiceInterfaces;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using System.Threading.Tasks;

namespace DiaryApp.API.Controllers.Users
{
    public class UserSettingsController : AppBaseController<UserSettingsController>
    {
        private readonly ICrudService<UserSettingsDto, UserSettings> _settingsService;
        public UserSettingsController(ICrudService<UserSettingsDto, UserSettings> settingsService, IMapper mapper, ILoggerFactory loggerFactory) : base(mapper, loggerFactory)
        {
            _settingsService = settingsService;
        }

        [HttpPut]
        [ProducesResponseType(StatusCodes.Status200OK)]
        public async Task<IActionResult> UpdateUserSettingsAsync([FromBody] UserSettingsDto userSettingsDto)
        {
            await _settingsService.UpdateAsync(userSettingsDto);
            return Ok();
        }
    }
}