using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using EPiServer.Core;
using EPiServer.Globalization;
using EPiServer.Web;
using EPiServer.Web.Routing;

namespace AlloyTemplates.Business.Extentions
{
    public static class PageReferenceExtentions
    {
        public static string GetFriendlyUrl(this PageReference pageReference)
        {
            var urlResolver = DependencyResolver.Current.GetService<UrlResolver>();

            return urlResolver.GetUrl(pageReference, ContentLanguage.PreferredCulture.ToString(), new VirtualPathArguments() { ContextMode = ContextMode.Default });
        }
    }
}