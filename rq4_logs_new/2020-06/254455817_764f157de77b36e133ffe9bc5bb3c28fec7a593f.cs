using System.Collections.Generic;
using System.Linq;
using Microsoft.AspNetCore.Mvc;
using SimpleMvcSitemap;
using WebStore.Domain.Entities;
using WebStore.Interfaces.Services;

namespace WebStore.Controllers
{
    public class SiteMapController : Controller
    {
        public IActionResult Index([FromServices] IProductService productService)
        {
            var nodes = new List<SitemapNode>
            {
                new SitemapNode(Url.Action("Index", "Home")),
                new SitemapNode(Url.Action("ContactUs", "Home")),
                new SitemapNode(Url.Action("Index", "Blog")),
                new SitemapNode(Url.Action("Index", "Catalog")),
                new SitemapNode(Url.Action("Index", "WebAPITest")),
            };

            nodes.AddRange(productService.GetCategories().Select(category => new SitemapNode(Url.Action("Index", "Catalog", new { CategoryId = category.Id }))));

            foreach (var brand in productService.GetBrands())
                nodes.Add(new SitemapNode(Url.Action("Index", "Catalog", new { BrandId = brand.Id })));

            foreach (var product in productService.GetProducts(new ProductFilter()))
                nodes.Add(new SitemapNode(Url.Action("View", "Catalog", new { product.Id })));

            return new SitemapProvider().CreateSitemap(new SitemapModel(nodes));
        }
    }
}