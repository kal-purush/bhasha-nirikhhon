using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using rcLogTransfers;
using rcLogWeb.Models;

namespace rcLogWeb.Controllers
{
    public class SistemaController : ControllerLog
    {
        public SistemaController(IHttpContextAccessor accessor)
            :base(accessor)
        {
        }

        [Authorize]
        [HttpGet]
        public async Task<IActionResult> Index()
        {
            ViewData["Usuario"] = UsuarioNome;

            SistemaModel sistemaModel = new SistemaModel(httpContext);

            SistemaTransfer sistemaTransfer = new SistemaTransfer();

            await sistemaModel.Consultar(sistemaTransfer);

            sistemaTransfer.Paginacao.PaginaInicial = 4;
            sistemaTransfer.Paginacao.PaginaAtual = 6;
            sistemaTransfer.Paginacao.PaginaFinal = 8;
            sistemaTransfer.Paginacao.RegistrosPorPagina = 4;
            sistemaTransfer.Paginacao.TotalPaginas = 15;
            sistemaTransfer.Paginacao.TotalRegistros = 59;

            return View(sistemaTransfer);
        }

        public IActionResult Error()
        {
            return View();
        }
    }
}