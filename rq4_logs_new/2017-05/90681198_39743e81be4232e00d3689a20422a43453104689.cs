using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using BangazonSiteMVC.Controllers;
using BangazonSiteMVC.Models;

namespace BangazonSiteMVC.DAL
{
    public class ProductRepository : IProductRepository
    {
        readonly AppContext _context;

        public ProductRepository(AppContext context)
        {
            _context = context;
        }
        public void Save(Product newProduct)
        {
            _context.Products.Add(newProduct);
            _context.SaveChanges();
        }
    }
}