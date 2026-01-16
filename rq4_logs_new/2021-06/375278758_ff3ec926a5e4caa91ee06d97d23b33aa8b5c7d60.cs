using System;

namespace BlazorWeatherAPI.Helpers
{
    public static class URIValid
    {
        //simple bool to test if the url provided is valid
        public static bool validURI(string thisUri )
        {
            Uri uriResult;
            bool theTest = !string.IsNullOrEmpty(thisUri) && Uri.TryCreate(thisUri, UriKind.Absolute, out uriResult) &&
            (uriResult.Scheme == Uri.UriSchemeHttp ||
            uriResult.Scheme == Uri.UriSchemeHttps);
            return theTest;
        }
    }
}