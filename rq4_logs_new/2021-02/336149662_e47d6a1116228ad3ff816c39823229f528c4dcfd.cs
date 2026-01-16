using QuieroPizza.Web.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Mvc;

namespace QuieroPizza.Web.Controllers
{
    public class ProductosController : Controller
    {
        // GET: Productos
        public ActionResult Index()
        {
            var producto1 = new ProductoModel();
            producto1.id = 1;
            producto1.Descripcion = "Pizzas 6 quesos";

            var producto2 = new ProductoModel();
            producto2.id = 2;
            producto2.Descripcion = "Pizzas 4 Estaciones";

            var producto3 = new ProductoModel();
            producto3.id = 3;
            producto3.Descripcion = "Pizzas Jamon y Queso";

            var listadeProductos = new List<ProductoModel>();
            listadeProductos.Add(producto1);
            listadeProductos.Add(producto2);
            listadeProductos.Add(producto3);

            return View(listadeProductos);
        }   
    }
}