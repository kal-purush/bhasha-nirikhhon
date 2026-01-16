namespace UrlShortener.Services.ShortUrlGenerator
{
    public class UniqueStringGenerator : IUniqueStringGenerator
    {
        private static readonly string str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890";

        private const int Length = 12;

        public string Generate()
        {
            return Guid.NewGuid()
                .ToByteArray()
                .Take(Length)
                .Select(b => str[b % str.Length].ToString())
                .Aggregate((x, y) => x + y);
        }
    }
}