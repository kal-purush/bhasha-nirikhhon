using FORO_D.Data;
using FORO_D.Models;
using FORO_D.ViewModels;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace FORO_D.Controllers
{
    public class AccountController : Controller
    {
        private readonly ForoContext _context;
        private readonly UserManager<Usuario> _userManager;
        private readonly SignInManager<Usuario> _signinManager;
        private readonly RoleManager<Rol> _rolManager;
        //private const string passDefault = "Password1!";
        private const string dominioAdministrador = "@admin.com";
       
        public AccountController(
            ForoContext context,
            UserManager<Usuario> userManager,
            SignInManager<Usuario> signinManager,
            RoleManager<Rol> rolManager
            )
        {
            this._context = context;
            this._userManager = userManager;
            this._signinManager = signinManager;
            this._rolManager = rolManager;
        }

        public ActionResult Registrar()
        {
            return View();
        }

        [Authorize(Roles = "Miembro")]
        [HttpPost]
        public async Task<ActionResult> Registrar(RegistracionViewModel viewModel)
        {

            if (ModelState.IsValid)
            {   
                Miembro miembroACrear = new ();
                miembroACrear.Nombre = viewModel.Nombre;
                miembroACrear.Apellido = viewModel.Apellido;
                miembroACrear.UserName = viewModel.UserName;
                miembroACrear.Email = viewModel.Email;
                miembroACrear.FechaAlta = DateTime.Now;
                

                var resultadoCreacion = await _userManager.CreateAsync(miembroACrear, viewModel.Password);

                if (resultadoCreacion.Succeeded)
                {
                   

                    if (!_rolManager.Roles.Any())
                    {
                        await CrearRolesBase();
                    }

                    var resultado = await _userManager.AddToRoleAsync(miembroACrear, "Miembro");

                    if (resultado.Succeeded)
                    {
                        await _signinManager.SignInAsync(miembroACrear, isPersistent: false);
                        return RedirectToAction("Edit", "Miembros", new { id = miembroACrear.Id });
                    }

                }

                foreach (var error in resultadoCreacion.Errors)
                {
                    ModelState.AddModelError(string.Empty, error.Description);
                }
            }

            return View(viewModel);
        }

        
        public ActionResult IniciarSesion(string returnurl)
        {
         
                TempData["returnUrl"] = returnurl;
     
            return View();
        }

        [HttpPost]
        public async Task<ActionResult> IniciarSesion(Login viewModel)
        {
            string returnUrl = TempData["returnUrl"] as string;

            if (ModelState.IsValid)
            {
                var usuario = _context.Usuarios.FirstOrDefault(p => p.Email == viewModel.Email || p.UserName==viewModel.Email);
                if (usuario == null)
                {
                    ModelState.AddModelError(string.Empty, "Inicio de sesión inválido");
                }
                else
                {
                    var resultadoSignIn = await _signinManager.PasswordSignInAsync(usuario, viewModel.Password, viewModel.Recordarme, false);
                    if (resultadoSignIn.Succeeded)
                    {
                        if (!string.IsNullOrEmpty(returnUrl))
                        {
                            return Redirect(returnUrl);
                        }

                        return RedirectToAction("Index", "Home");
                    }
                    else
                    {
                        ModelState.AddModelError(string.Empty, "Inicio de sesión inválido");
                    }
                }
                
            }
            return View(viewModel);
        }

        public async Task<ActionResult> CerrarSesion()
        {
            await _signinManager.SignOutAsync();

            return RedirectToAction("Index", "home");
        }

        [HttpGet]
        public IActionResult AccesoDenegado()
        {
            return View();
        }


        public async Task CrearRolesBase()
        {
            List<string> roles = new () { "Usuario", "Miembro"};

            if (!_context.Roles.Any())
            {
                foreach (string rol in roles)
                {
                    await CrearRole(rol);
                }
            }
        }

        private async Task CrearRole(string rolName)
        {
            if (!await _rolManager.RoleExistsAsync(rolName))
            {
                await _rolManager.CreateAsync(new Rol(rolName));
            }
        }

        [Authorize(Roles = "Usuario")]
        public ActionResult CrearAdministrador()
        {
            return View();
        }

        [Authorize(Roles = "Usuario")]
        [HttpPost]
        public async Task<ActionResult> CrearAdministrador(RegistracionAdminViewModel viewModel)
        {
            //Hago con model lo que necesito.

            if (ModelState.IsValid)
            {
                Usuario usuarioACrear = new ();
                usuarioACrear.Nombre = viewModel.Nombre;
                usuarioACrear.Apellido = viewModel.Apellido;
                usuarioACrear.Email = viewModel.UserName + dominioAdministrador;
                usuarioACrear.UserName = viewModel.UserName;
                usuarioACrear.FechaAlta = DateTime.Now;
                
                var resultadoCreacion = await _userManager.CreateAsync(usuarioACrear, viewModel.Password);

                if (resultadoCreacion.Succeeded)
                {

                    if (!_rolManager.Roles.Any())
                    {
                        await CrearRolesBase();
                    }

                    var resultado = await _userManager.AddToRoleAsync(usuarioACrear, "Usuario");

                    if (resultado.Succeeded)
                    {
                        return RedirectToAction("Index", "Usuarios", new { id = usuarioACrear.Id });
                    }
                }

                foreach (var error in resultadoCreacion.Errors)
                {
                    ModelState.AddModelError(string.Empty, error.Description);
                }
            }

            return View(viewModel);
        }

        [HttpGet]
        public async Task<IActionResult> EmailDisponible(string email)
        {
            if (_signinManager.IsSignedIn(User) && User.IsInRole("Usuario"))
            {
                var emailUsado = await _userManager.FindByIdAsync(email);
                if (email.Length > 10 && (email.Substring(email.Length - 10) == "@admin.com"))
                {
                    if (emailUsado == null)
                    {
                        return Json(true);
                    }
                    else
                    {
                        return Json($"El correo {email} ya está en uso!");
                    }
                }
                else
                {
                    return Json($"El correo {email} es invalido, Ingrese en el siguiente formato: Example@admin.com");
                }
            }
            else
            {
                var emailUsado = _context.Usuarios.Any(p => p.Email == email);

                if (email.Length > 10 && !(email.Substring(email.Length - 10) == "@admin.com") && !emailUsado)
                {
                    return Json(true);
                }
                else
                {
                    return Json($"El correo {email} ya está en uso o no corresponde al tipo de mail valido ");
                }
            }

        }

        [HttpGet]
        public IActionResult UsuarioDisponible(string username)
        {
            //var usuarioUsado = _userManager.FindByIdAsync(username);
            var usuarioUsado = _context.Usuarios.FirstOrDefault(p => p.UserName==username);
            if (usuarioUsado == null)
            {
                return Json(true);
            }
            else
            {
                return Json($"El Usuario {username} ya está en uso.");
            }
        }
    }
}