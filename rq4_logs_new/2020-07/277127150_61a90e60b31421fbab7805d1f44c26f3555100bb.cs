using System.Net.Mime;
using System.Threading.Tasks;
using AutoMapper;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Skim.API.Entities;
using Skim.API.Helpers;
using Skim.API.Models;
using Skim.API.Services;

namespace Skim.API.Controllers
{
    [ApiController]
    [Consumes(MediaTypeNames.Application.Json)]
    [Route("")]
    public class MainController : ControllerBase
    {
        private readonly ISkimRepository _skimRepository;
        private readonly IMapper _mapper;

        public MainController(ISkimRepository skimRepository, IMapper mapper)
        {
            _skimRepository = skimRepository;
            _mapper = mapper;
        }
        
        [HttpHead("{shortString}")]
        [HttpGet("{shortString}")]
        [ProducesResponseType(StatusCodes.Status200OK)]
        [ProducesResponseType(StatusCodes.Status404NotFound)]
        public async Task<ActionResult<ShortLinkDto>> GetByShortStringAsync(string shortString)
        {
            var shortLinkFromRepository = await _skimRepository.GetShortLinkAsync(shortString);

            if (shortLinkFromRepository == null)
            {
                return NotFound();
            }

            return _mapper.Map<ShortLinkDto>(shortLinkFromRepository);
        }
        
        [HttpPost]
        [ProducesResponseType(StatusCodes.Status201Created)]
        [ProducesResponseType(StatusCodes.Status400BadRequest)]
        [ProducesResponseType(StatusCodes.Status409Conflict)]
        public async Task<ActionResult<ShortLinkDto>> AddAsync(ShortLinkDto shortLinkDto)
        {
            if (await _skimRepository.ShortStringExistsAsync(shortLinkDto.ShortString))
            {
                return Conflict();
            }

            if (!Validator.IsValidUrl(shortLinkDto.FullLink))
            {
                return BadRequest();
            }

            var shortLinkFromRepository = await _skimRepository.AddShortLinkAsync(_mapper.Map<ShortLink>(shortLinkDto));

            return CreatedAtAction(actionName: nameof(GetByShortStringAsync),
                routeValues: new {shortString = shortLinkFromRepository.ShortString},
                value: _mapper.Map<ShortLinkDto>(shortLinkFromRepository));
        }
    }
}