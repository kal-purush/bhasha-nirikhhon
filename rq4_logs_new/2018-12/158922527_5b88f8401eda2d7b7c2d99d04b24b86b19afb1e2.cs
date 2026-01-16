using CommandPattern.Commands;

namespace CommandPattern
{
    public class Invoker
    {
        private readonly ICommand _onCommand;
        private readonly ICommand _offCommand;
        private readonly ICommand _dimUpCommand;
        private readonly ICommand _dimDownCommand;

        public Invoker(ICommand onCommand, ICommand offCommand, ICommand dimUpCommand, ICommand dimDownCommand)
        {
            _onCommand = onCommand;
            _offCommand = offCommand;
            _dimUpCommand = dimUpCommand;
            _dimDownCommand = dimDownCommand;
        }

        void On()
        {
            _onCommand.Execute();
        }

        void Off()
        {
            _offCommand.Execute();
        }

        void DimUp()
        {
            _dimUpCommand.Execute();
        }

        void DimDown()
        {
            _dimDownCommand.Execute();
        }
    }
}