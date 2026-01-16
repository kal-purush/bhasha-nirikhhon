using amazon_backend.Services.Random;
using Slugify;

namespace amazon_backend.Services.SlugService
{
    public class SlugService : ISlugService
    {
        private readonly IRandomService _randomService;
        private readonly ISlugHelper _slugHelper;

        private readonly string DASH_DELIMETER = "-";
        private readonly int MAXIMUM_WORDS = 5;

        public SlugService(IRandomService randomService, ISlugHelper slugHelper)
        {
            _randomService = randomService;
            _slugHelper = slugHelper;
        }
        public string GetSlug(string item)
        {
            var randomString = _randomService.ConfirmCode(4);
            var slug = _slugHelper.GenerateSlug(item);
            var slugAr = slug.Split(DASH_DELIMETER);
            if (slugAr.Length < MAXIMUM_WORDS)
            {
                return slug + DASH_DELIMETER + randomString;
            }
            return string.Join(DASH_DELIMETER, slugAr.Take(MAXIMUM_WORDS)) + DASH_DELIMETER + randomString;
        }
    }
}