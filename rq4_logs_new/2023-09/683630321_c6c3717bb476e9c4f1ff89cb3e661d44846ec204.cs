namespace Web.User.Helpers
{
    public class FileHelper
    {
        public string UploadImage(string imageData, string sourceFileName, string userId, string destinationDirectory)
        {
            var rawImage = imageData.Contains(",") ? imageData.Substring(imageData.IndexOf(',') + 1) : imageData;
            var imageBytes = Convert.FromBase64String(rawImage);
            var extension = Path.GetExtension(sourceFileName);
            var destinationFile = $"{userId}{extension}";
            var destinationPath = Path.Combine(destinationDirectory, destinationFile);
            File.WriteAllBytes(destinationPath, imageBytes);
            return destinationFile;
        }

        public bool IsProfileImageExists(string imageName, string destinationDirectory)
        {
            var destinationPath = Path.Combine(destinationDirectory, imageName);
            return File.Exists(destinationPath);
        }
    }
}