using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;
using WebMVC.Models;
using Newtonsoft.Json;
using Microsoft.Extensions.Configuration;

namespace WebMVC.Controllers
{
    public class UsuariosController : Controller
    {
        public string UrlRoles { get; set; }
        public string UrlUsuarios { get; set; }
        public UsuariosController(IConfiguration configuration)
        {
            UrlRoles = configuration.GetValue<string>("UrlRoles");
            UrlUsuarios = configuration.GetValue<string>("UrlUsuarios");
           
        }

        [HttpGet]
        public ActionResult Login()
        {
            return View();
        }

        [HttpPost]
        public ActionResult Login(UsuarioDTOViewModel vm)
        {
            //PENDIENTE VALIDAR QUE EL USUARIO EXISTE Y TIENE ESE ROL
            HttpContext.Session.SetString("rol", vm.usuario.Email);
            ViewBag.Mensaje = "Bienvenido " + vm.usuario.Email;
            return View();
        }

        // GET: AutoresWebapiController/Create
        public ActionResult Create()
        {
                     
            return View();
        }

        // POST: AutoresWebapiController/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create(UsuarioDTOViewModel vm)
        {
            try
            {
               
                HttpClient cliente = new HttpClient();

               
                Task<HttpResponseMessage> tarea1 = cliente.PostAsJsonAsync(UrlUsuarios, vm.usuario);
                tarea1.Wait();

                HttpResponseMessage respuesta = tarea1.Result;

                if (respuesta.IsSuccessStatusCode) //status code de la serie 200
                {
                    return RedirectToAction(nameof(Login));
                }
                else
                {
                    ViewBag.Error = "No se pudo dar de alte el usuario. Error: " + ObtenerBody(respuesta);
                    
                    return View();
                }
            }
            catch (Exception e)
            {
                ViewBag.Error = "Ocurrió un error: " + e.Message;
                //loguear la excepción? inner exception?
                return View(vm);
            }
        }
        private string ObtenerBody(HttpResponseMessage respuesta)
        {
            HttpContent contenido = respuesta.Content;

            Task<string> tarea2 = contenido.ReadAsStringAsync();
            tarea2.Wait();

            return tarea2.Result;
        }
    }
}