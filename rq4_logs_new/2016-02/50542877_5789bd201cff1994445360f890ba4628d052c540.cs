using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Text.RegularExpressions;
using System.Web.Http;
using Examine;
using Examine.Providers;
using Examine.SearchCriteria;
using Skybrud.Umbraco.SelfService.Constants;
using Skybrud.Umbraco.SelfService.Helpers;
using Skybrud.Umbraco.SelfService.Install;
using Skybrud.WebApi.Json;
using Skybrud.WebApi.Json.Meta;
using Umbraco.Core.Media;
using Umbraco.Core.Models;
using Umbraco.Web.WebApi;
using Skybrud.Umbraco.SelfService.Extensions;

namespace Skybrud.Umbraco.SelfService.Controllers.Api {

    [JsonOnlyConfiguration]
    public class SelfServiceAdminController : UmbracoAuthorizedApiController {

        [HttpGet]
        public object Install(int parentId = -1) {

            List<string> log = new List<string>();

            new SelfServiceInstaller().Run(parentId, log);

            return new {
                success = true,
                log
            };

        }

        [HttpGet]
        public object GetCategoriesContext(string ids = null) {

            // Get the category root node
            IPublishedContent root = SelfServiceHelpers.GetContentFromGuid("42895027-98a3-49c9-845d-493abb34d5f9");

            // Convert the IDs string into an array
            int[] array = StringHelpers.CsvToInt(ids);

            List<object> temp = new List<object>();

            Dictionary<int, IContentType> contentTypes = new Dictionary<int, IContentType>();

            foreach (int id in array) {
                
                IPublishedContent content = UmbracoContext.ContentCache.GetById(id);

                if (content == null) continue;

                IContentType contentType;
                if (!contentTypes.TryGetValue(content.DocumentTypeId, out contentType)) {
                    IContentType ct = ApplicationContext.Services.ContentTypeService.GetContentType(content.DocumentTypeId);
                    if (ct != null) contentTypes.Add(ct.Id, ct);
                }

                temp.Add(new {
                    id = content.Id,
                    name = content.Name,
                    icon = contentType == null ? "icon-tag" : contentType.Icon
                });
            
            }

            return new {
                startNodeId = (root == null ? 0 : root.Id),
                selected = temp.ToArray()
            };

        }

        [HttpGet]
        public object GetCategoriesTree() {

            // Get the category root node
            IPublishedContent root = SelfServiceHelpers.GetContentFromGuid("42895027-98a3-49c9-845d-493abb34d5f9");

            // Parse the children (recursively)
            return root.Children.Select(SimplifyCategoryWithChildren);
        
        }


        [HttpGet]
        public object GetActionPages(string text = null, string categories = null, int limit = 30, int page = 1, string sort = null, string order = null) {

            // Get a reference to the parent node holding the action pages
            IPublishedContent parent = SelfServiceHelpers.GetContentFromGuid(SelfServiceConstants.Pages.ActionPages);

            // Throw an error if unable to find the parent node
            if (parent == null) {
                return Request.CreateResponse(JsonMetaResponse.GetError(
                    HttpStatusCode.InternalServerError,
                    "Unable to locate action pages."
                ));
            }

            int[] categoryIds = StringHelpers.CsvToInt(categories);









            List<string> query = new List<string>();

            // Get a reference to the searcher
            BaseSearchProvider searcher = ExamineManager.Instance.SearchProviderCollection["InternalSearcher"];

            // Limit the search to action pages
            query.Add("nodeTypeAlias:SkySelfServiceActionPage");


            if (!String.IsNullOrWhiteSpace(text)) {

                // Fields to search for the keywords
                List<string> fields = new List<string> { "nodeName", "title" };

                string lciQuery = PrepareSearchString(text);

                query.Add(String.Join(" AND ", (
                    from term in lciQuery.Split(new[] { ' ' }, StringSplitOptions.RemoveEmptyEntries)
                    let escaped = (
                        from field in fields
                        select String.Format("{1}:({0} {0}*)", term, Lucene.Net.QueryParsers.QueryParser.Escape(field))
                    )
                    select "(" + String.Join(" OR ", escaped) + ")"
                )));

            }

            if (categoryIds.Length > 0) {
                query.Add("(" + String.Join(" OR ", from categoryId in categoryIds select "skySelfServiceCategories:" + categoryId) + ")");
            }








            // Create a new search criteria and set our query
            ISearchCriteria criteria = searcher.CreateSearchCriteria();
            criteria = criteria.RawQuery(String.Join(" AND ", query));

            // Make the search in Examine
            ISearchResults results = searcher.Search(criteria);

            int pages = (int) Math.Ceiling(results.TotalItemCount / (double) limit);
            page = Math.Max(1, Math.Min(page, pages));

            int offset = (page * limit) - limit;

            IEnumerable<SearchResult> hai = results;


            string sortField = sort;
            order = order == "desc" ? "desc" : "asc";

            switch (sortField) {
                case "id":
                    hai = hai.OrderBy(x => x.Id, order == "desc");
                    break;
                case "name":
                    hai = hai.OrderBy(x => x.Fields["nodeName"], order == "desc");
                    break;
                case "type":
                    hai = hai.OrderBy(x => x.Fields["nodeTypeAlias"], order == "desc");
                    break;
                case "created":
                    hai = hai.OrderBy(x => x.Fields["createDate"], order == "desc");
                    break;
                case "updated":
                    hai = hai.OrderBy(x => x.Fields["updateDate"], order == "desc");
                    break;
                case "creator":
                    hai = hai.OrderBy(x => x.Fields["creatorName"], order == "desc");
                    break;
                case "writer":
                    hai = hai.OrderBy(x => x.Fields["writerName"], order == "desc");
                    break;
                default:
                    sortField = "name";
                    hai = hai.OrderBy(x => x.Fields["nodeName"], order == "desc");
                    break;
            }




            hai = hai.Skip(offset).Take(limit);

            return new {
                meta = new {
                    code = 200
                },
                context = new {
                    parentId = parent.Id
                },
                pagination = new {
                    page,
                    pages,
                    limit,
                    offset,
                    total = results.TotalItemCount,
                    from = offset + 1,
                    to = Math.Min(offset + limit, results.Count())
                },
                sorting = new {
                    sortField,
                    order
                },
                data = (
                    from result in hai
                    let content = UmbracoContext.ContentCache.GetById(result.Id)
                    select new {
                        id = result.Id,
                        name = result.Fields["nodeName"],
                        updated = DateTime.ParseExact(result.Fields["updateDate"].Substring(0, 14), "yyyyMMddHHmmss", CultureInfo.InvariantCulture).ToString("yyyy-MM-dd HH:mm"),
                        updatedBy = new {
                            id = Int32.Parse(result.Fields["writerID"]),
                            name = result.Fields["writerName"]
                        },
                        url = content == null ? null : content.Url
                    }
                ),
                query
            };

        }

        private string PrepareSearchString(string query) {
            return Regex.Replace(query ?? "", @"[^\wæøåÆØÅ\- ]", "").ToLowerInvariant().Trim();
        }

        private object SimplifyCategoryWithChildren(IPublishedContent content) {
            return new {
                id = content.Id,
                level = content.Level - 3,
                name = content.Name,
                icon = "icon-tag",
                children = from child in content.Children select SimplifyCategoryWithChildren(child)
            };
        }

    }

}