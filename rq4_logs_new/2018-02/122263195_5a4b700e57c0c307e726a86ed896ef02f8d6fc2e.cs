using HtmlAgilityPack;
using System.Linq;
using System.Web.Mvc;

namespace ServerPushViewEngine
{
    internal static class PreloadHeaderGenerator
    {
        internal static void CreatePreloadHeader(
            ViewContext context, 
            HtmlDocument document,
            bool requirePrefix,
            string type, 
            string attribute,
            string preloadType, 
            params string[] secondaryAttributes
        ) {
            var tags = document.DocumentNode.SelectNodes($"//{type}");
            if (tags == null) return;

            foreach (var tag in tags)
            {
                if (tag.HasAttributes && tag.Attributes.Count(attr => attr.Name.ToLower() == attribute) == 1)
                {
                    //-----------------------
                    // If we're passing secondary attributes, make sure this
                    // element has those attributes
                    //-----------------------
                    if (secondaryAttributes.Count() > 0 && !tag.Attributes.Any(attr => secondaryAttributes.Contains(attr.Value.ToLower())))
                    {
                        continue;
                    }

                    //-----------------------
                    // If the view engine was setup with the RequirePrefix
                    // property set, check to see if that attribute is in this
                    // tag before continuing
                    //-----------------------
                    if (requirePrefix && !tag.Attributes.Any(attr => attr.Name.ToLower() == "data-http2-push" && attr.Value.ToLower() == "true"))
                    {
                        continue;
                    }

                    //-----------------------
                    // If this is a tag that points at an external server, don't
                    // include that file in the HTTP/2 push headers
                    //-----------------------
                    var href = tag.Attributes.First(attr => attr.Name.ToLower() == attribute).Value;
                    if (href.ToLower().StartsWith("http") && href.ToLower().StartsWith("//"))
                    {
                        continue;
                    }

                    //-----------------------
                    // Finally, add our preload header!
                    //-----------------------
                    context.HttpContext.Response.AddHeader("Link", $"<{href}>; rel=preload; as={preloadType}");
                }
            }
        }
    }
}