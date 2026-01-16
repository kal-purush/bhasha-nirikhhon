namespace EncyclopediaGalactica.SourceFormats.Controllers.SourceFormatNode;

using System.Net;
using Dtos;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;
using Sdk.Models.SourceFormatNode;

public partial class SourceFormatNodeController
{
    [HttpPut]
    [Route("update")]
    [ProducesResponseType(typeof(SourceFormatNodeUpdateResponseModel), StatusCodes.Status200OK)]
    [ProducesResponseType(StatusCodes.Status400BadRequest)]
    [ProducesResponseType(StatusCodes.Status404NotFound)]
    [ProducesResponseType(StatusCodes.Status500InternalServerError)]
    public async Task<ActionResult<SourceFormatNodeUpdateResponseModel>> Update(
        [FromBody] SourceFormatNodeDto? dto)
    {
        if (dto is null)
            _logger.LogWarning("{Dto} is null ", dto);

        SourceFormatNodeUpdateResponseModel responseModel = await _sourceFormatsService
            .SourceFormatNode
            .UpdateSourceFormatNodeAsync(dto)
            .ConfigureAwait(false);

        switch (responseModel.HttpStatusCode)
        {
            case (int)HttpStatusCode.OK:
                return Ok(responseModel);

            case (int)HttpStatusCode.BadRequest:
                return BadRequest(responseModel);

            case (int)HttpStatusCode.NotFound:
                return NotFound(responseModel);

            case (int)HttpStatusCode.InternalServerError:
                return Problem("Error", "Error happened", (int)HttpStatusCode.InternalServerError);

            default:
                return Problem("Default error", "Default error happened", (int)HttpStatusCode.InternalServerError);
        }
    }
}