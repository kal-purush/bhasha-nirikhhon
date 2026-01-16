using System;
using System.Collections.Generic;
using System.Data.Entity;
using System.Linq;
using System.Net;
using System.Web;
using System.Web.Mvc;
using mvcCrudOperation.Models;

namespace mvcCrudOperation.Controllers
{
    public class ProductAjaxController : Controller
    {
        private PosContextDatabase db = new PosContextDatabase();
        // GET: Product
        public ActionResult Index()
        {
            var product = db.Products.ToList();
            return View(product);
        }

        [HttpGet]
        public ActionResult Create()
        {
            return View();
        }

        [HttpPost]
        public ActionResult Create(ProductViewModel productViewModel)
        {

            Product product = new Product();

            product.Name = productViewModel.Name;
            product.Code = productViewModel.Code;
            product.Price = productViewModel.Price;

            db.Products.Add(product);
            var result = db.SaveChanges();
            if (result > 0)
            {
                return RedirectToAction("Edit", new {@id = product.Id});
            }
            return View();
        }


        [HttpPost]
        public ActionResult Edit(ProductViewModel productViewModel)
        {

            //Product product = db.Products.FirstOrDefault(x => x.Id == productViewModel.Id);
            Product product = new Product();

            product.Id = productViewModel.Id;
            product.Name = productViewModel.Name;
            product.Code = productViewModel.Code;
            product.Price = productViewModel.Price;

            db.Entry(product).State = EntityState.Modified;
            var result = db.SaveChanges();
            if (result > 0)
            {
                TempData["msg_edit"] = "Record has been updated successfully.";
            }
            return View();
        }

        [HttpGet]
        public ActionResult Edit(int? id)
        {
            if (id == null)
            {
                return new HttpStatusCodeResult(HttpStatusCode.BadRequest);
            }
            Product product = db.Products.FirstOrDefault(x => x.Id == id);
            ProductViewModel productViewModel = new ProductViewModel();

            productViewModel.Id = product.Id;
            productViewModel.Name = product.Name;
            productViewModel.Code = product.Code;
            productViewModel.Price = product.Price;
            return View(productViewModel);
        }

        [HttpGet]
        public ActionResult Delete(int id)
        {
            Product product = db.Products.FirstOrDefault(x => x.Id == id);
            ProductViewModel productViewModel = new ProductViewModel();

            productViewModel.Id = product.Id;
            productViewModel.Name = product.Name;
            productViewModel.Code = product.Code;
            productViewModel.Price = product.Price;
            return View(productViewModel);
        }

        [HttpPost]
        public ActionResult Delete(ProductViewModel productViewModel)
        {

            Product product = db.Products.FirstOrDefault(x => x.Id == productViewModel.Id);
            //Product product = new Product();
            var result = 0;
            if (product != null)
            {
                db.Entry(product).State = EntityState.Deleted;
                result = db.SaveChanges();
            }

            if (result > 0)
            {
                return RedirectToAction("Index");
            }
            return View();
        }

        [HttpGet]
        public ActionResult DeleteMultiple()
        {
            var data = db.Products.ToList();
            List<ProductViewModel> ViewModels = new List<ProductViewModel>();
            foreach (var product in data)
            {
                ProductViewModel productViewModel = new ProductViewModel();
                productViewModel.Id = product.Id;
                productViewModel.Name = product.Name;
                productViewModel.Code = product.Code;
                productViewModel.Price = product.Price;
                ViewModels.Add(productViewModel);
            }
            return View(ViewModels);
        }

        [HttpPost]
        public ActionResult DeleteMultiple(IEnumerable<ProductViewModel> products)
        {
            foreach (var product in products)
            {
                if (product.IsDeleted)
                {
                    var data = db.Products.FirstOrDefault(x => x.Id == product.Id);
                    db.Entry(data).State = EntityState.Deleted;
                }
            }
            db.SaveChanges();
            return RedirectToAction("DeleteMultiple");
        }
    }
}