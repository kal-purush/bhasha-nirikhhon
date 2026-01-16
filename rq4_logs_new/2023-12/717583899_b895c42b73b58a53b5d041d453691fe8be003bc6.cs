using EcommerceCCO2023.Models;
using EcommerceCCO2023.Models.Data;
using Microsoft.AspNetCore.Mvc;

namespace EcommerceCCO2023.Controllers
{
    public class LoginController : Controller
    {
        public IActionResult Login()
        {
            return View();
        }
        [HttpPost]
        public IActionResult verificaUsuario(Cliente cliente)
        {
            ClienteData clienteData = new ClienteData();
            if(clienteData.VerificaUsuario(cliente.Email, cliente.Senha))
            {
                return RedirectToAction("Index", "Home");
            } else
            {
                return RedirectToAction("Login");
            }

            
        }
    }
}