using System.Collections.Generic;
using Entities;

namespace PublishingFacade
{
    internal class PublishingFacadeFeature : IPublishingFacadeFeature
    {
        private readonly ICollection<ICommand> _commands;

        public PublishingFacadeFeature(params ICommand[] commands)
        {
            _commands = commands;
        }

        public string Name => "publishingfacade";
        public IEnumerable<ICommand> Commands => _commands;
        public void Enable()
        {
        }

        public void Disable()
        {
        }
    }
}