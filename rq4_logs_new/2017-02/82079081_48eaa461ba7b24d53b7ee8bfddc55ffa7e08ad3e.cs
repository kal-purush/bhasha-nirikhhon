using Contoso.Shop.EFTests.Shop;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Contoso.Shop.EFTests.Controllers
{
    public class HomeController : Controller
    {
        private readonly ShopContext shopContext;

        public HomeController(ShopContext shopContext)
        {
            this.shopContext = shopContext;
        }

        public IActionResult Index()
        {
            var produtos = shopContext.Produtos.ToList();

            return Ok(produtos);
        }
        public IActionResult Create(string nome, decimal preco)
        {
            var produto = new Produto
            {
                Nome = nome,
                Preco = preco,
                CriadoEm = DateTimeOffset.Now
            };

            shopContext.Produtos.Add(produto);
            shopContext.SaveChanges();
            return Ok(produto);
        }
    }
}