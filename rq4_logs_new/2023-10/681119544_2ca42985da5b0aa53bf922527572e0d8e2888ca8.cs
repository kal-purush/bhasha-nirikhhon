using System.Net.Mime;
using Blackbird.Applications.Sdk.Common;
using RestSharp;
using File = Blackbird.Applications.Sdk.Common.Files.File;

namespace Apps.HTTP.Models.Responses;

public class FileResponseDto : ResponseDto
{
    public FileResponseDto(RestResponse response) : base(response)
    {
        var contentDisposition = response.ContentHeaders.FirstOrDefault(header => header.Name == "Content-Disposition");
        ContentFile = new(response.RawBytes)
        {
            Name = contentDisposition != null && contentDisposition.Value.ToString().Contains("attachment;") ? 
                contentDisposition.Value.ToString().Split('"')[1] : Guid.NewGuid().ToString(),
            ContentType = response.ContentHeaders?
                .FirstOrDefault(x => x.Name == "Content-Type")?.Value?.ToString() ?? MediaTypeNames.Application.Octet
        };
    }
    
    [Display("Content file")]
    public File ContentFile { get; set; }
}