using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using LogicaAplicacion.InterfacesCasosUso;
using Microsoft.Extensions.Configuration;
using System.Net.Http;
using LogicaNegocio.Dominio;
using Newtonsoft.Json;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace WebMVC.Controllers
{
    public class SeleccionesApiController : Controller
    {

        public string UrlApiSelecciones { get; set; }
        public IListadoPaises CUListadoPaises { get; set; }

        public SeleccionesApiController(IConfiguration conf, IListadoPaises cuListadoPaises)
        {
            UrlApiSelecciones = conf.GetValue<string>("UrlApiSelecciones");
            CUListadoPaises = cuListadoPaises;
        }

        // GET: SeleccionesApiController
        public ActionResult Index()
        {
            try
            {
                HttpClient cli = new HttpClient();
                Task<HttpResponseMessage> t1 = cli.GetAsync(UrlApiSelecciones);
                HttpResponseMessage res = t1.Result;
                string txt = ObtenerBody(res);
                if (res.IsSuccessStatusCode)
                {
                    List<Seleccion> selecciones = JsonConvert.DeserializeObject<List<Seleccion>>(txt);
                    return View(selecciones);

                }
                else
                {
                    ViewBag.Error = "No se obtienen selecciones. Error: " + res.ReasonPhrase + txt;
                    return View(new List<Seleccion>());
                }
            }
            catch (Exception e)
            {
                ViewBag.Error = "Ups! " + e.Message;
                return View(new List<Seleccion>());
            }
        }

        private string ObtenerBody(HttpResponseMessage respuesta)
        {
            HttpContent contenido = respuesta.Content;

            Task<string> t2 = contenido.ReadAsStringAsync();
            t2.Wait();
            return t2.Result;
        }

        // GET: SeleccionesApiController/Details/5
        public ActionResult Details(int id)
        {
           
            return View();
        }

        // GET: SeleccionesApiController/Create
        public ActionResult Create()
        {
            return View();
        }

        // POST: SeleccionesApiController/Create
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Create(IFormCollection collection)
        {
            try
            {
                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }

        // GET: SeleccionesApiController/Edit/5
        public ActionResult Edit(int id)
        {
            return View();
        }

        // POST: SeleccionesApiController/Edit/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Edit(int id, IFormCollection collection)
        {
            try
            {
                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }

        // GET: SeleccionesApiController/Delete/5
        public ActionResult Delete(int id)
        {
            return View();
        }

        // POST: SeleccionesApiController/Delete/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Delete(int id, IFormCollection collection)
        {
            try
            {
                return RedirectToAction(nameof(Index));
            }
            catch
            {
                return View();
            }
        }
    }
}