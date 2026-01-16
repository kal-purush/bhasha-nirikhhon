using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.RazorPages;
using Hospital.App.Dominio;
using Hospital.App.Persistencia;

namespace Hospital.App.Frontend.Pages
{
    public class CrearSugerenciaModel : PageModel
    {
        private IRepositorioSugerenciasCuidados _repositorioSugerencias = new RepositorioSugerenciasCuidado( new Hospital.App.Persistencia.AppContext());
        private IRepositorioPaciente _repositorioPaciente = new RepositorioPaciente( new Hospital.App.Persistencia.AppContext());
        private IRepositorioHistoriaClinica _repoHistoria = new RepositorioHistoriaClinica(new Hospital.App.Persistencia.AppContext());


        [BindProperty]
        public HistoriaClinica HistoriaClinica{get;set;}
        [BindProperty]
        public Paciente paciente {get;set;}
        public int idHistoria {get;set;}
        public int idpaciente {get;set;}
        public SugerenciasCuidado SugerenciasCuidado{get;set;}

        public CrearSugerenciaModel()
        {}

        public ActionResult OnPost()
        {
            try{
            SugerenciasCuidado sugerenciaAdicionada = _repositorioSugerencias.AddSugerenciasCuidado(SugerenciasCuidado);
            return RedirectToPage("../SugerenciasCuidado");
            }catch(System.Exception e)
            {
                ViewData["Error"] = "Error: " + e.Message;
                return Page();
            }
            
        }
        public ActionResult OnGet()
        {
            return Page();
        }
    }
}