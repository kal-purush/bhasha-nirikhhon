using fichaTecnica.Data;
using fichaTecnica.Historias.CadastrarInsumos;
using Microsoft.AspNetCore.Mvc;
using System.Linq;
using System.Threading.Tasks;

namespace fichaTecnica.Controllers
{
    public class ItemInsumoController : Controller
    {
        public readonly DataContext contexto;

        public ItemInsumoController(DataContext contexto)
        {
            this.contexto = contexto;
        }

        public IActionResult Index()
        {
            return View(contexto.ItemInsumos.ToList());
        }

        public IActionResult Cadastrar() => View();

        [HttpPost]
        public async Task<IActionResult> Cadastrar(
            [FromServices] CadastrarItemInsumos cadastrarItemInsumos,
            CadastrarItemInsumosViewModel cadastrarItemInsumosViewModel)
        {
            if (!ModelState.IsValid)
                return View(cadastrarItemInsumosViewModel);

            await cadastrarItemInsumos.Executar(cadastrarItemInsumosViewModel);
            return RedirectToAction(nameof(Index));
        }
    }
}