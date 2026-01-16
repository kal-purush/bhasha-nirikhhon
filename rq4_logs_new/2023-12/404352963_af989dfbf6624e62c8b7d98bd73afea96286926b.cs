using MediatR;
using System;
using System.Threading.Tasks;

namespace SME.Worker.Agendador.Aplicacao
{
    public class PropagarCacheUseCase : IPropagarCacheUseCase
    {
        protected readonly IMediator mediator;

        public PropagarCacheUseCase(IMediator mediator)
        {
            this.mediator = mediator ?? throw new ArgumentNullException(nameof(mediator));
        }

        public async Task Executar()
        {
            await mediator.Send(new PublicarFilaSerapEstudantesCommand(RotasRabbitSerap.PropagarCache, Guid.NewGuid()));
        }
    }
}