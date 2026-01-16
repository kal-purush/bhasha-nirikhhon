using Microsoft.AspNetCore.Mvc;
using Finanzas.Model;
using Finanzas.Data;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.Authentication;
using System.Security.Claims;
using Newtonsoft.Json;

namespace Finanzas.Controllers
{
    public class AccesoController : Controller
    {
        
        private readonly AppDbContext _db;
        public AccesoController(AppDbContext db) { _db = db; }

        public Usuario Usuario { get; set; }
        public Cuenta cuenta { get; set; }
        public IActionResult Index()
        {
            return View();
        }


        [HttpPost]
        public async Task<IActionResult> Index(Usuario _usuario)
        {
            var usuario = _db.ValidarUsuario(_usuario.Correo, _usuario.Clave, _usuario.Nombre);
            if (usuario != null)
            {
                var claims = new List<Claim>
                {
                    new Claim(ClaimTypes.Name, _usuario.Nombre),
                    new Claim(ClaimTypes.Email, usuario.Correo),
                    new Claim(ClaimTypes.NameIdentifier, Convert.ToString(usuario.Id)),
                    new Claim("Rol", usuario.Rol)
                };
                
                var claimsIdentity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);

                await HttpContext.SignInAsync(CookieAuthenticationDefaults.AuthenticationScheme, new ClaimsPrincipal(claimsIdentity));

                HttpContext.Session.SetString("SessionUser", JsonConvert.SerializeObject(usuario));

                return RedirectToAction("Index", "Home");  
            }
            else
            {
                ModelState.AddModelError("Nombre", "Usuario no encontrado");
                return View(_usuario);
            }
        }

        public async Task<IActionResult> Salir()
        {
            await HttpContext.SignOutAsync(CookieAuthenticationDefaults.AuthenticationScheme);
            return RedirectToAction("Index", "Acceso");
        }
        public IActionResult Registro()
        {
            return View();
        }

        [HttpPost]
        public async Task<IActionResult> Registro(Usuario _usuario)
        {
            var usuario = _db.ValidarUsuario(_usuario.Correo, _usuario.Clave, _usuario.Nombre);
            if (usuario == null)
            {
                //crear usuario
                _usuario.Rol = "cliente";
                _db.Add(_usuario);
                Usuario = _usuario;
                _db.SaveChanges();

                //crear cuenta a usuario
                cuenta = new Cuenta();
                cuenta.Saldo = 0;
                cuenta.NCuenta = 1;
                cuenta.usuario = _usuario;
                _db.Add(cuenta);
                _db.SaveChanges();


                var claims = new List<Claim>
                {
                    new Claim(ClaimTypes.Name, _usuario.Nombre),
                    new Claim("Correo", _usuario.Correo),
                    new Claim("Rol", _usuario.Rol)
                };

                var claimsIdentity = new ClaimsIdentity(claims, CookieAuthenticationDefaults.AuthenticationScheme);

                await HttpContext.SignInAsync(CookieAuthenticationDefaults.AuthenticationScheme, new ClaimsPrincipal(claimsIdentity));
                HttpContext.Session.SetString("SessionUser", JsonConvert.SerializeObject(_usuario));

                return RedirectToAction("Index", "Home");
            }
            else
            {
                ModelState.AddModelError("Nombre", "El usuario ya esta registrado");
                return View(_usuario);
            }
        }

    }
}