using MediatR;
using Rydo.Framework.MediatR.Eventos;
using Rydo.Framework.MediatR.Handlres;

namespace EventBus.Abstractions
{
    public interface IEventBus
    {
        void Subscribe<T>() where T : IRequest<Unit>;

        void Publish(IRequest<Unit> @event);
    }
}