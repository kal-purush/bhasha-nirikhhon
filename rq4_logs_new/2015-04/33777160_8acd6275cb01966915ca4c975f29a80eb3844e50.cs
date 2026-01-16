using HeroisDoCancer.ContextoDados;
using HeroisDoCancer.Models;
using HeroisDoCancer.Services;
using HeroisDoCancer.ViewModels;
using System;
using System.Web.Mvc;

namespace HeroisDoCancer.Controllers
{
    public class VoluntarioController : Controller
    {
        private VoluntarioService service;

        public VoluntarioController()
        {
            var contexto = new DadosEmMemoria();
            this.service = new VoluntarioService(contexto);
        }

        [HttpPost]
        public ActionResult Cadastro(Voluntario voluntario)
        {
            var result = new RespostaOperacaoViewModel();

            try
            {
                this.service.Cadastra(voluntario);
            }
            catch (Exception ex)
            {
                result.Erro = true;
                result.Mensagem = ex.Message;
            }
            
            return Json(result);
        }
    }
}