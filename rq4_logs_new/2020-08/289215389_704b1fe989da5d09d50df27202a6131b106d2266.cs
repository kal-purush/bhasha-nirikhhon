using System;
using AgendaEstudos.Models;
using AgendaEstudos.Models.Repository;
using Microsoft.AspNetCore.Mvc;

namespace AgendaEstudos.Controllers {
    public class SessaoEstudoController : Controller {

        private readonly ITarefaRepository _repository;

        public SessaoEstudoController(ITarefaRepository repo) {
            _repository = repo;
        }
        
        public ViewResult IniciarSessao(long id)
            => View("NovaSessao", 
                new SessaoEstudoViewModel {
                    Tarefa = _repository.GetById(id),
                    HorarioInicio = DateTime.Now
                });

        public IActionResult ConcluirSessao(SessaoEstudoViewModel sessaoEstudo) {
            sessaoEstudo.HorarioFim = DateTime.Now;
            
            Console.WriteLine("SessaoEstudo: " + sessaoEstudo);
            
            Tarefa tarefa = _repository.GetById(sessaoEstudo.Tarefa.TarefaID);
            
            Console.WriteLine("Tarefa antes: " + tarefa);

            tarefa.HorasEstudadas += sessaoEstudo.DuracaoSessao;
            
            Console.WriteLine("Tarefa depois: " + tarefa);

            
            _repository.Atualizar(tarefa);

            return Redirect("/tarefas/listar");
        }
    }
}