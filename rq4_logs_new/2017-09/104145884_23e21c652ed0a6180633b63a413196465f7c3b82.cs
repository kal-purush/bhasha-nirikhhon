namespace HardySoft.GpsTracker.Extensions
{
    using System;

    /// <summary>
    /// Extension class for page navigation purpose.
    /// </summary>
    internal static class PageTokenExtension
    {
        /// <summary>
        /// Convert the page type to navigate to page token name.
        /// </summary>
        /// <param name="pageType">The strongly-typed page type.</param>
        /// <returns>The page token supported by Prism.</returns>
        public static string GetPageToken(this Type pageType)
        {
            var typeName = pageType.Name;

            if (!typeName.EndsWith("Page", StringComparison.Ordinal))
            {
                throw new ArgumentException("Only view page type could be used here.");
            }

            return typeName.Replace("Page", string.Empty);
        }
    }
}