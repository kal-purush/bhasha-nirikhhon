using System;
using Microsoft.Extensions.Logging;
using Soenneker.Tags.Util.Abstract;

namespace Soenneker.Tags.Util;

public class TagsUtil : ITagsUtil
{
    private readonly ILogger<TagsUtil> _logger;

    public TagsUtil(ILogger<TagsUtil> logger)
    {
        _logger = logger ?? throw new ArgumentNullException(nameof(logger));
    }

    public void OpenFile(string filePath)
    {
        try
        {
            // Open the file using a using statement to ensure proper disposal.
            using (var file = TagLib.File.Create(filePath))
            {
                _logger.LogInformation("Opened file: {FilePath}", filePath);
                _logger.LogInformation("Title: {Title}", file.Tag.Title);
                _logger.LogInformation("Artists: {Artists}", string.Join(", ", file.Tag.Performers));
            }
        }
        catch (Exception ex)
        {
            // Log any errors encountered while opening or processing the file.
            _logger.LogError(ex, "An error occurred while opening the file: {FilePath}", filePath);
        }
    }
}