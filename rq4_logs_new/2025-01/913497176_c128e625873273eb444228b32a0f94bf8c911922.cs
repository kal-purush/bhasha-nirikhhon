using Microsoft.AspNetCore.Hosting;

namespace LibraryWebApp.Helpers
{
    /// <summary>
    /// Allows manipulation of book cover images
    /// </summary>
    public class BookCoverImageManager : IBookCoverImageManager
    {
        private readonly string _wwwRootPath;
        private readonly string _bookCoverImagesRelativePath;

        public BookCoverImageManager(IWebHostEnvironment webHostEnvironment)
        {
            _wwwRootPath = webHostEnvironment.WebRootPath;
            _bookCoverImagesRelativePath = @"\images\bookCoverImages";
        }

        public string DefaultNoBookCoverImagePath { get => _bookCoverImagesRelativePath + @"\no_cover.jpg"; }

        /// <summary>
        /// Saves the book cover image with a unique name to the disk
        /// </summary>
        /// <param name="coverImage"></param>
        /// <returns>The path (relative to the wwwroot folder) to which the image was saved</returns>
        /// <exception cref="ArgumentNullException"></exception>
        public async Task<string> SaveBookCoverImageToDiskAsync(IFormFile coverImage)
        {
            if (coverImage == null)
            {
                throw new ArgumentNullException("No book cover image was provided");
            }

            string randomGuid = Guid.NewGuid().ToString();
            string fileExtension = Path.GetExtension(coverImage.FileName);

            string newFileName = randomGuid + fileExtension;

            string fileRelativePath = Path.Combine(_bookCoverImagesRelativePath, newFileName);

            string fileFullPath = _wwwRootPath + fileRelativePath;

            using (var fs = new FileStream(fileFullPath, FileMode.Create))
            {
                await coverImage.CopyToAsync(fs);
            }

            return fileRelativePath;
        }

        /// <summary>
        /// Deletes the book cover image from the disk
        /// </summary>
        /// <param name="coverImageUrl">The path of the book cover image (relative to wwwroot)</param>
        /// <exception cref="ArgumentNullException"></exception>
        public void DeleteBookCoverImage(string coverImageUrl)
        {
            string fileFullPath = _wwwRootPath + coverImageUrl;

            if (File.Exists(fileFullPath) == false)
            {
                throw new ArgumentNullException("The book cover image does not exist");
            }

            File.Delete(fileFullPath);
        }
    }
}