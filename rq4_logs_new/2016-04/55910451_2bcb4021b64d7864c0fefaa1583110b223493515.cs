using DesignPatterns.BehaviorPatterns.Mediator.Mediator;

namespace DesignPatterns.BehaviorPatterns.Mediator.Collegue
{
    public abstract class Collegue
    {
        private readonly AbstractMediator _mediator;


        protected Collegue(AbstractMediator mediator)
        {
            _mediator = mediator;
        }


        public void Send(string msg)
        {
            _mediator.Send(msg, this);
        }

        public abstract void Notify(string msg);
    }
}