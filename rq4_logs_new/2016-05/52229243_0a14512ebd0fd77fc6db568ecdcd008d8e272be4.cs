using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using System.Web;
using System.Web.Http.Routing;

namespace ExpenseTracker.API.Helpers
{
    internal class VersionConstraint : IHttpRouteConstraint
    {
        public const string VersionHeaderName = "api-version";
        private const int DefaultVersion = 1;
        public int AllowedVersion { get; private set; }

        public VersionConstraint(int allowedVersion)
        {
            AllowedVersion = allowedVersion;
        }

        public bool Match(HttpRequestMessage request, IHttpRoute route, string parameterName, IDictionary<string, object> values,
            HttpRouteDirection routeDirection)
        {
            if (routeDirection == HttpRouteDirection.UriResolution)
            {
                var version = GetVersionFromCustomRequestHeader(request);

                if (version == null)
                {
                    version = GetVersionFromCustomContentType(request);
                }

                return ((version ?? DefaultVersion) == AllowedVersion);
            }

            return true;
        }

        private int? GetVersionFromCustomContentType(HttpRequestMessage request)
        {
            var mediaTypes = request.Headers.Accept.Select(h => h.MediaType);

            var regEx = new Regex(@"application\/vnd\.expensetrackerapi\.v([\d]+)\+json");

            var matchingMediaType = mediaTypes.FirstOrDefault(mediaType => regEx.IsMatch(mediaType));

            if (matchingMediaType == null) return null;

            var m = regEx.Match(matchingMediaType);
            var versionAsString = m.Groups[1].Value;

            int version;
            if (versionAsString != null && int.TryParse(versionAsString, out version))
            {
                return version;
            }

            return null;
        }

        private int? GetVersionFromCustomRequestHeader(HttpRequestMessage request)
        {
            string versionAsString;
            IEnumerable<string> headerValues;
            if (request.Headers.TryGetValues(VersionHeaderName, out headerValues) && headerValues.Count() == 1)
            {
                versionAsString = headerValues.FirstOrDefault();
            }
            else
            {
                return null;
            }

            int version;
            if (versionAsString != null && int.TryParse(versionAsString, out version))
            {
                return version;
            }

            return null;
        }
    }
}