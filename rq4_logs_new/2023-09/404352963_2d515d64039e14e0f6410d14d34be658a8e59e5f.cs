using MediatR;
using SME.Worker.Agendador.Aplicacao.Comandos;
using System;
using System.Threading.Tasks;

namespace SME.Worker.Agendador.Aplicacao.CasosDeUso.EncaminhamentoNAAPA
{
    public class NotificarInatividadeDoAtendimentoNAAPAUseCase : AbstractUseCase, INotificarInatividadeDoAtendimentoNAAPAUseCase
    {
        public NotificarInatividadeDoAtendimentoNAAPAUseCase(IMediator mediator) : base(mediator)
        {
        }

        public async Task Executar()
        {
            await mediator.Send(new PublicaFilaRabbitCommand(RotasRabbitSgp.ExecutarNotificacaoInatividadeAtendimentoNAAPA, Guid.NewGuid()));
        }
    }
}