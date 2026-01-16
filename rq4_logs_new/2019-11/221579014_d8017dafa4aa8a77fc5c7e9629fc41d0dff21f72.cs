using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using Microsoft.EntityFrameworkCore;
using ChajdPizzaWebApp.Data;
using ChajdPizzaWebApp.Models;
using ChajdPizzaWebApp.Repositories;

namespace ChajdPizzaWebApp.Controllers
{
    public class OrderDetailsController : Controller
    {
        private readonly OrderDetailsRepo _repo;

        public OrderDetailsController(OrderDetailsRepo repo)
        {
            _repo = repo;
        }

        // GET: OrderDetails
        public async Task<IActionResult> Index()
        {
            var orderDetails = await _repo.SelectAll();
            return View(orderDetails);
        }

        // GET: OrderDetails/Details/5
        public async Task<IActionResult> Details(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var orderDetail = await _repo.SelectOrderAllDetails(id);
            if (orderDetail == null)
            {
                return NotFound();
            }

            return View(orderDetail);
        }

        // GET: OrderDetails/Create
        public IActionResult Create()
        {
           // ViewData["OrderId"] = new SelectList(_context.Orders, "Id", "Id");
            //ViewData["SizeId"] = new SelectList(_context.Size, "Id", "BaseSize");
            return View();
        }

        // POST: OrderDetails/Create
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("Id,OrderId,SizeId,ToppingsSelected,ToppingsCount,Price,SpecialRequest")] OrderDetail orderDetail)
        {
            if (ModelState.IsValid)
            {
                await _repo.Add(orderDetail);
                
                return RedirectToAction(nameof(Index));
            }
            //ViewData["OrderId"] = new SelectList(_context.Orders, "Id", "Id", orderDetail.OrderId);
            //ViewData["SizeId"] = new SelectList(_context.Size, "Id", "BaseSize", orderDetail.SizeId);
            return View(orderDetail);
        }

        // GET: OrderDetails/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }

            var orderDetail = await _repo.SelectById(id);
            if (orderDetail == null)
            {
                return NotFound();
            }
            //ViewData["OrderId"] = new SelectList(_repo.SelectById(id), "Id", "Id", orderDetail.OrderId);
            //ViewData["SizeId"] = new SelectList(_context.Size, "Id", "BaseSize", orderDetail.SizeId);
            return View(orderDetail);
        }

        // POST: OrderDetails/Edit/5
        // To protect from overposting attacks, please enable the specific properties you want to bind to, for 
        // more details see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("Id,OrderId,SizeId,ToppingsSelected,ToppingsCount,Price,SpecialRequest")] OrderDetail orderDetail)
        {
            if (id != orderDetail.Id)
            {
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                try
                {
                    await _repo.Update(orderDetail);
                    
                }
                catch (DbUpdateConcurrencyException)
                {
                    if (!OrderDetailExists(orderDetail.Id))
                    {
                        return NotFound();
                    }
                    else
                    {
                        throw;
                    }
                }
                return RedirectToAction(nameof(Index));
            }
            //ViewData["OrderId"] = new SelectList(_context.Orders, "Id", "Id", orderDetail.OrderId);
            //ViewData["SizeId"] = new SelectList(_context.Size, "Id", "BaseSize", orderDetail.SizeId);
            return View(orderDetail);
        }

        // GET: OrderDetails/Delete/5
        public async Task<IActionResult> Delete(int? id)
        {
            if (id == null)
            {
                return NotFound();
            }
            var orderDetail = await _repo.SelectById(id);
            if (orderDetail == null)
            {
                return NotFound();
            }

            return View(orderDetail);
        }

        // POST: OrderDetails/Delete/5
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id)
        {
            var orderDetail = await _repo.SelectById(id);
            await _repo.Remove(orderDetail);
            
            return RedirectToAction(nameof(Index));
        }

        private bool OrderDetailExists(int id)
        {
            return _repo.OrderDetailExists(id);
        }
    }
}