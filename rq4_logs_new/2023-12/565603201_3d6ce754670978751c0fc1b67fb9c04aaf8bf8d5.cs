using System.Text.RegularExpressions;
using SmallsOnline.Web.Lib.Services;

public static partial class ImageSyntaxParser
{
    [GeneratedRegex(@"!\[(?'imageDescription'.+?)\]\((?'imagePath'((?'httpProto'https|http):\/\/.+?|.+?))\)")]
    private static partial Regex ImageSyntaxRegex();

    public static async Task<string> ReplaceAndUploadImagesAsync(string markdownText, string markdownFilePath, IBlobStorageService blobStorageService)
    {
        var imageSyntaxMatches = ParseImageSyntax(markdownText);

        if (imageSyntaxMatches is null)
        {
            return markdownText;
        }

        foreach (Match match in imageSyntaxMatches)
        {
            if (match.Groups["httpProto"].Success)
            {
                continue;
            }

            string imageFilePath = match.Groups["imagePath"].Value;

            string imageAbsolutePath = Path.GetFullPath(imageFilePath, Path.GetDirectoryName(markdownFilePath)!);

            if (!File.Exists(imageAbsolutePath))
            {
                throw new FileNotFoundException($"The image file '{imageAbsolutePath}' does not exist.", imageAbsolutePath);
            }

            FileInfo imageFile = new(imageAbsolutePath);

            string mimeType = GetImageMimeType(imageFile);

            using FileStream fileStream = imageFile.Open(FileMode.Open, FileAccess.Read);

            string uploadedImageUrl = await blobStorageService.UploadBlogImageAsync(imageFile.Name, mimeType, fileStream);

            markdownText = markdownText.Replace(imageFilePath, uploadedImageUrl);

            fileStream.Close();
        }

        return markdownText;
    }

    private static MatchCollection? ParseImageSyntax(string markdownText)
    {
        if (!ImageSyntaxRegex().IsMatch(markdownText))
        {
            return null;
        }

        return ImageSyntaxRegex().Matches(markdownText);
    }

    private static string GetImageMimeType(FileInfo imageFile)
    {
        string mimeType = imageFile.Extension switch
        {
            ".png" => "image/png",
            ".jpg" or ".jpeg" => "image/jpeg",
            ".gif" => "image/gif",
            ".bmp" => "image/bmp",
            ".tiff" => "image/tiff",
            ".ico" => "image/x-icon",
            ".svg" => "image/svg+xml",
            ".webp" => "image/webp",
            ".avif" => "image/avif",
            ".heif" => "image/heif",
            ".heic" => "image/heic",
            _ => throw new NotSupportedException($"The image file extension '{imageFile.Extension}' is not supported.")
        };

        return mimeType;
    }
}