using Api.Common.Routing;
using Api.Controllers.Common;
using Application.Contract.Services;
using Application.DTOs.Tag;
using Microsoft.AspNetCore.Mvc;

namespace Api.Controllers
{
    [ApiController]
    public class TagController : AppControllerBase
    {
        private readonly ITagServices _tagServices;

        public TagController(ITagServices tagServices)
        {
            _tagServices = tagServices;
        }

        [HttpGet]
        [Route(TagRoutes.GetById)]
        public async Task<ActionResult> GetById([FromRoute] Guid id)
        {
            var response = await _tagServices.GetTagByIdAsync(id);

            return ResponseHandler(response);
        }

        [HttpGet]
        [Route(TagRoutes.GetAll)]
        public async Task<ActionResult> GetAll()
        {
            var response = await _tagServices.GetAllTagsAsync();

            return ResponseHandler(response);
        }

        [HttpPost]
        [Route(TagRoutes.Create)]
        public async Task<IActionResult> Create([FromForm] CreateTagDto dto)
        {
            var response = await _tagServices.CreateTagAsync(dto);

            return ResponseHandler(response);
        }

        [HttpPut]
        [Route(TagRoutes.Update)]
        public async Task<IActionResult> Put([FromRoute] Guid id, [FromForm] UpdateTagDto dto)
        {
            var response = await _tagServices.UpdateTagAsync(id, dto);

            return ResponseHandler(response);
        }

        [HttpDelete]
        [Route(TagRoutes.Delete)]
        public async Task<IActionResult> Delete([FromRoute] Guid id)
        {
            var response = await _tagServices.DeleteTagAsync(id);

            return ResponseHandler(response);
        }
    }
}