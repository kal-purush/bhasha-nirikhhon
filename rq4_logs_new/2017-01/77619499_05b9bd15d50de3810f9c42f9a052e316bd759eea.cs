// <copyright file="ElasticsearchClient.cs">
//     Copyright (c) Andrey Igumnov. All rights reserved.
//     Licensed under the Apache License Version 2.0. See LICENSE file in the project root for full license information.
// </copyright>

namespace ElasticAlert.Data
{
    using System;
    using System.IO;
    using System.Net;
    using System.Text;
    using Newtonsoft.Json.Linq;

    /// <summary>
    /// Represents Elasticsearch client
    /// </summary>
    public sealed class ElasticsearchClient
    {
        /// <summary>
        /// Service URI
        /// </summary>
        private readonly Uri serviceUri;

        /// <summary>
        /// Initializes a new instance of the <see cref="ElasticsearchClient"/> class
        /// </summary>
        /// <param name="serviceUri">Service URI</param>
        public ElasticsearchClient(Uri serviceUri)
        {
            this.serviceUri = serviceUri;
        }

        /// <summary>
        /// Returns documets count
        /// </summary>
        /// <param name="query">Search query</param>
        /// <returns>Documents count</returns>
        public int Count(string query)
        {
            var uriBuilder = new UriBuilder(this.serviceUri);
            uriBuilder.Path = uriBuilder.Path + "_count";
            var countResponse = Search(uriBuilder.Uri, query);
            dynamic d = JObject.Parse(countResponse);
            return d.count;
        }

        private static string Search(Uri uri, string query)
        {
            using (var webClient = new WebClient())
            {
                try
                {
                    var res = webClient.DownloadString(uri + "?q=" + query);
                    return res;
                }
                catch (WebException exception)
                {
                    if (exception.Response != null)
                    {
                        using (var stream = exception.Response.GetResponseStream())
                        {
                            var reader = new StreamReader(stream, Encoding.UTF8);
                            throw new ApplicationException(reader.ReadToEnd());
                        }
                    }
                    throw;
                }
            }
        }
    }
}