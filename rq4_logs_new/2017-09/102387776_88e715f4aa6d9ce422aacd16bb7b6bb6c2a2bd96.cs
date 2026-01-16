using System.IO;
using System.Linq;
using CoolBytes.Core.Interfaces;

namespace CoolBytes.Core.Factories
{
    public class PhotoFactoryValidator : IPhotoFactoryValidator
    {
        private const int MaxFileSize = (1024 * 1024) * 3;
        private readonly string[] _allowedContentTypes;

        public PhotoFactoryValidator()
        {
            _allowedContentTypes = new []
            {
                "image/jpeg",
                "image/jpg",
                "image/png"
            };
        }

        public bool Validate(Stream stream, string contentType) => 
            _allowedContentTypes.Any(c => c == contentType) && (stream.Length <= MaxFileSize && stream.Length > 0);
    }
}