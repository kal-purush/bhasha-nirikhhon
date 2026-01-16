using AgendaEstudos.Models;
using Microsoft.AspNetCore.Mvc;
using AgendaEstudos.Services ;

namespace AgendaEstudos.Controllers {
    public class AdminController : Controller {
        
        private readonly IStatsService _service;

        public AdminController(IStatsService service) {
            _service = service;
        }
        
        // GET
        [HttpGet]
        public ViewResult Index() 
            => View(new AdminViewModel {
                Tarefas = _service.Tarefas
                
            });

        [HttpPost]
        public IActionResult AtribuirMetas(AdminViewModel model) {
            _service.AtribuirMetasProporcionais(model.Meta);
            _service.RepoTarefas.Atualizar(_service.Tarefas);
            return Redirect("/Tarefas/Listar");
        }

        [HttpPost]
        public IActionResult ReiniciarTodos() {
            _service.RepoTarefas.ReiniciarHorasEstudadas(
                _service.RepoTarefas.ListarTarefas());
            return RedirectToAction("Index");
        }

        [HttpPost]
        public IActionResult DeletarTodos() {
            _service.RepoTarefas.DeletarTarefas(
                _service.RepoTarefas.ListarTarefas());
            return RedirectToAction("Index");
        }
    }
}