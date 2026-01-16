using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using EveryBook.Models;
using Microsoft.AspNetCore.Http;
using EveryBook.Data;
using System.Runtime.Serialization.Formatters.Binary;
using System.IO;
using Newtonsoft.Json;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc.Rendering;

namespace EveryBook.Controllers
{
    [Authorize]
    public class CartController : Controller
    {
        private readonly EveryBookContext _context;
        public CartController(EveryBookContext context)
        {
            _context = context;
        }
        public IActionResult Index()
        {
            List<Book> cart;
            if (HttpContext.Session.Get(GetUniqueSessionKey("BooksInCart")) == null)
            {
                cart = new List<Book>();
                HttpContext.Session.SetString(GetUniqueSessionKey("BooksInCart"), JsonConvert.SerializeObject(cart));
                HttpContext.Session.SetInt32(GetUniqueSessionKey("NumOfBooksInCart"), 0);
                HttpContext.Session.SetInt32(GetUniqueSessionKey("TotalToPay"), 0);
            }
            else
            {
                cart = JsonConvert.DeserializeObject<List<Book>>(HttpContext.Session.GetString(GetUniqueSessionKey("BooksInCart")));
            }
            ViewData["DistributionUnitId"] = new SelectList(_context.DistributionUnit, "Id", "Name");
            return View(cart.ToList());
        }

        [HttpPost]
        public ActionResult AddToCart(int id)
        {
            List<Book> cart;
            Book bookToAdd = _context.Book.Where(b => b.Id == id).FirstOrDefault();
            if (HttpContext.Session.Get(GetUniqueSessionKey("BooksInCart")) == null)
            {
                cart = new List<Book>();
                HttpContext.Session.SetString(GetUniqueSessionKey("BooksInCart"), JsonConvert.SerializeObject(cart));
            }
            else
            {
                cart = JsonConvert.DeserializeObject<List<Book>>(HttpContext.Session.GetString(GetUniqueSessionKey("BooksInCart")));
            }

            if (isBookAlreadyInCart(id) == -1)
            {
                cart.Add(bookToAdd);
            }

            HttpContext.Session.SetString(GetUniqueSessionKey("BooksInCart"), JsonConvert.SerializeObject(cart));
            UpdateNumOfBooksInCart(true);
            UpdateTotalToPay(true, bookToAdd.Price);

            return RedirectToAction("Index");
        }

        [HttpPost]
        public ActionResult RemoveFromCart(int id)
        {
            List<Book> cart = JsonConvert.DeserializeObject<List<Book>>(HttpContext.Session.GetString(GetUniqueSessionKey("BooksInCart")));
            Book bookToRemove = _context.Book.Where(b => b.Id == id).FirstOrDefault();
            if (isBookAlreadyInCart(id) != -1)
            {
                UpdateNumOfBooksInCart(false);
                UpdateTotalToPay(false, bookToRemove.Price);
                cart.RemoveAt(isBookAlreadyInCart(id));

                HttpContext.Session.SetString(GetUniqueSessionKey("BooksInCart"), JsonConvert.SerializeObject(cart));
            }
            return RedirectToAction("Index");
        }

        private void UpdateNumOfBooksInCart(bool isAddAction)
        {
            if (HttpContext.Session.Get(GetUniqueSessionKey("NumOfBooksInCart")) == null)
            {
                HttpContext.Session.SetInt32(GetUniqueSessionKey("NumOfCartItems"), 1);
            }
            else
            {
                var NumOfBooksInCart = (int)HttpContext.Session.GetInt32(GetUniqueSessionKey("NumOfBooksInCart"));
                if(isAddAction)
                {
                    NumOfBooksInCart++;
                }
                else
                {
                    NumOfBooksInCart--;
                }
                HttpContext.Session.SetInt32(GetUniqueSessionKey("NumOfBooksInCart"), NumOfBooksInCart);
            }
        }

        private void UpdateTotalToPay(bool isAddAction, int bookPrice)
        {
            if (HttpContext.Session.Get(GetUniqueSessionKey("TotalToPay")) == null)
            {
                HttpContext.Session.SetInt32(GetUniqueSessionKey("TotalToPay"), bookPrice);
            }
            else
            {
                int TotalToPay = (int)HttpContext.Session.GetInt32(GetUniqueSessionKey("TotalToPay"));
                if (isAddAction)
                {
                    TotalToPay += bookPrice;
                }
                else
                {
                    TotalToPay -= bookPrice;
                }
                HttpContext.Session.SetInt32(GetUniqueSessionKey("TotalToPay"), TotalToPay);
            }
        }

        private string GetUniqueSessionKey(string key)
        {
            return HttpContext.User.Identity.Name.ToString() + key;
        }

        private int isBookAlreadyInCart(int id)
        {
            List<Book> cart = JsonConvert.DeserializeObject<List<Book>>(HttpContext.Session.GetString(GetUniqueSessionKey("BooksInCart")));
            for (int i = 0; i < cart.Count; i++)
            {
                if (cart[i].Id == id)
                {
                    return i;
                }
            }
            return -1;
        }
    }
}