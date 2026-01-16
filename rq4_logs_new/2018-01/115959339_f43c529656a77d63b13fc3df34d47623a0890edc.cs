using System.Collections.Generic;

namespace Administration.Adapters
{
    internal class CommandPresenter : ICommandPresenter
    {
        private readonly ICollection<ICommandView> _views;

        public CommandPresenter()
        {
            _views = new List<ICommandView>();
        }

        public void ShowCommands(IEnumerable<string> commands)
        {
            foreach (var commandView in _views)
            {
                commandView.ShowAll(commands);
            }
        }

        public void AttachView(ICommandView view)
        {
            _views.Add(view);
        }
    }
}