using EFOptimization.DataAccess;
using EFOptimization.Models.DTO;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;
using System.Data.Entity;

namespace EFOptimization.Controllers
{
  public class OptimizationController : Controller
  {
    // GET: Optimization
    public ActionResult GetProductsList()
    {
      var awContext = new AdventureWorks();

      var productList = awContext.Products.Select(p => new ProductListModel()
      {
        Name = p.Name,
        ListPrice = p.ListPrice,
        ProductID = p.ProductID,
        InventoryCount = p.ProductInventories.Count()
      }).ToList();

      return View("Index");
    }
    public ActionResult GetProductsFullList()
    {
      var awContext = new AdventureWorks();

      var productFullList = awContext.Products.Include(p => p.ProductInventories).ToList();

      return View("Index");
    }
  }
}