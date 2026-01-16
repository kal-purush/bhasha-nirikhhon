// <copyright file="ApiCompareCommandArguments.cs" company="Endjin Limited">
// Copyright (c) Endjin Limited. All rights reserved.
// </copyright>

namespace Endjin.SemVer.DotNetApi.Cli
{
    /// <summary>
    /// The command line arguments for the compare tool.
    /// </summary>
    internal class ApiCompareCommandArguments
    {
        /// <summary>
        /// Create a <see cref="ApiCompareCommandArguments"/>.
        /// </summary>
        /// <param name="feedUrl">The NuGet <see cref="FeedUrl"/>.</param>
        /// <param name="packageFolder">The <see cref="PackageFolder"/>.</param>
        public ApiCompareCommandArguments(string feedUrl, string packageFolder)
        {
            this.FeedUrl = feedUrl;
            this.PackageFolder = packageFolder;
        }

        /// <summary>
        /// Gets the URL of the NuGet feed to search for earlier versions.
        /// </summary>
        public string FeedUrl { get; }

        /// <summary>
        /// Gets the path of the local folder containing the newly-built packages, which are to be
        /// compared with published earlier versions.
        /// </summary>
        public string PackageFolder { get; }
    }
}