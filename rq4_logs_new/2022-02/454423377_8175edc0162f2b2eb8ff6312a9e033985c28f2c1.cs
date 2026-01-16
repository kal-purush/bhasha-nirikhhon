using GameShopProject.Services.Implementations;
using GameShopProject.Services.Interfaces;
using Microsoft.AspNetCore.Mvc;

namespace GameShopProject.Controllers;

public class OrderController : Controller
{
    private readonly IOrderService _service;

    public OrderController(OrderService service)
    {
        _service = service;
    }

    public IActionResult MakeOrder()
    {
        _service.MakeOrder();

        return View();
    }
}