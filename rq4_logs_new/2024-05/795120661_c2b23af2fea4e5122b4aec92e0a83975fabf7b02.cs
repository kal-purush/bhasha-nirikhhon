#nullable enable
using System.Collections.Generic;
using Daipan.Core.Interfaces;
using VContainer;
using VContainer.Unity;

namespace Daipan.Core
{
    public sealed class Updater : ITickable
    {
        IEnumerable<IUpdate> _updates = null!;

        void ITickable.Tick()
        {
            foreach (var update in _updates) update.Update();
        }

        [Inject]
        public void Initialize(
            IEnumerable<IUpdate> Updates
        )
        {
            _updates = Updates;
        }
    }
}