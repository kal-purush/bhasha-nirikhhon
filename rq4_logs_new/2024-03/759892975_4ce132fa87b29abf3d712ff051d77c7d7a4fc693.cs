using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using YouCode.BE;
using YouCode.BL;

namespace YouCode.GUI.Controllers
{
    public class AdminController : Controller
    {
        // creación de objetos de acceso a la capa BL
        AdminBL adminBL = new AdminBL();
        UserBL userBL = new UserBL();

        // Acción que muestra la lista de administradores
        public async Task<IActionResult> Index()
        {
            var admins = await adminBL.GetAllAsync();
            return View(admins);
        }

        // Acción que muestra los detalles de un administrador
        public async Task<IActionResult> Details(int id)
        {
            var admin = await adminBL.GetByIdAsync(new Admin { Id = id });
            return View(admin);
        }

        // Acción que muestra el formulario para crear un nuevo administrador
        public IActionResult Create()
        {
            return View();
        }

        // Acción que recibe los datos del formulario y los envía a la base de datos para crear un nuevo administrador
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create(Admin admin)
        {
            try
            {
                await adminBL.CreateAsync(admin);
                return RedirectToAction(nameof(Index));
            }
            catch (Exception ex)
            {
                ViewBag.Error = ex.Message;
                return View(admin);
            }
        }


        // Acción que muestra el formulario para editar un administrador existente
        public async Task<IActionResult> Edit(int id)
        {
            
            return View();
        }

        // Acción que recibe los datos del formulario y los envía a la base de datos para editar un administrador existente
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, Admin admin)
        {
            try
            {
                await adminBL.UpdateAsync(admin);
                return RedirectToAction(nameof(Index));
            }
            catch (Exception ex)
            {
                ViewBag.Error = ex.Message;
                return View(admin);
            }
        }

        // Acción que muestra los datos de un administrador para confirmar su eliminación
        public async Task<IActionResult> Delete(Admin admin)
        {
            var adminDB = await adminBL.GetByIdAsync(admin);
           
         
            return View(adminDB);
        }

        // Acción que recibe la confirmación para eliminar un administrador
        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(int id, Admin admin)
        {
            try
            {
                await adminBL.DeleteAsync(admin);
                return RedirectToAction(nameof(Index));
            }
            catch (Exception ex)
            {
                ViewBag.Error = ex.Message;
                var adminDB = await adminBL.GetByIdAsync(admin);
                return View(admin);
            }
        }
    }
}
