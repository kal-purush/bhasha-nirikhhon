using Microsoft.AspNetCore.Mvc;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;

namespace fichaTecnica.Controllers
{
    public class BaseController : Controller
    {
        public object NotificarSucesso(string mensagem = "")
        {
            if (string.IsNullOrEmpty(mensagem))
                TempData["sucesso"] = "Ação realizada com sucesso!";
            else
                TempData["sucesso"] = mensagem;

            return TempData["sucesso"];
        }

        public object NotificarErros(IEnumerable<string> erros)
        {
            if (erros.Any())
            {
                TempData["erros"] = JsonSerializer.Serialize(erros);
            }
            else
                TempData["erros"] = "Algo inesperado ocorreu! Tente novamente. Se o problema persistir, contate a administração.";

            return TempData["erros"];
        }

        public object NotificarAtencao(string mensagem = "")
        {
            TempData["atencao"] = mensagem;

            return TempData["atencao"];
        }

        public object NotificarInformacao(string mensagem = "")
        {
            TempData["informacao"] = mensagem;

            return TempData["informacao"];
        }

        public void NotificarErrosDaModelState()
        {
            var erros = new List<string>();
            ModelState.Values.ToList().ForEach(x => x.Errors.ToList().ForEach(y => erros.Add(y.ErrorMessage)));
            NotificarErros(erros);
        }

        public void Notificar(Dictionary<string, string> Erros)
        {
            if (!Erros.Any())
                NotificarSucesso();
            else
                NotificarErros(Erros.Values);
        }
    }
}