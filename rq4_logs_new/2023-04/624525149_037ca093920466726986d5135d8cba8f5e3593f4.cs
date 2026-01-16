using Microsoft.AspNetCore.Mvc;
using TG.Backend.Features.Blob.CreateBlobs;
using TG.Backend.Features.Blob.GetBlobById;

namespace TG.Backend.Controllers;

[ApiController]
[Route("[controller]")]
public class BlobController : ControllerBase
{
    private readonly ISender _sender;

    public BlobController(ISender sender)
    {
        _sender = sender;
    }

    [HttpGet("{id:Guid}")]
    public async Task<IActionResult> GetBlob([FromRoute] Guid id)
    {
        var blobResponse = await _sender.Send(new GetBlobByIdQuery(id));

        if (blobResponse is not { IsSuccess: true, StatusCode: HttpStatusCode.OK })
            return StatusCode(StatusCodes.Status503ServiceUnavailable);
        
        return blobResponse.Blob!;
    }

    [HttpPost]
    public async Task<IActionResult> CreateBlobs([FromForm] IFormFileCollection blobs)
    {
        var blobResponse = await _sender.Send(new CreateBlobsCommand(blobs));

        if (blobResponse is { IsSuccess: true, StatusCode: HttpStatusCode.NoContent })
            return NoContent();

        return StatusCode(StatusCodes.Status503ServiceUnavailable);
    }
}