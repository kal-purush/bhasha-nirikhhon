using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ICSheet5e.ViewModels
{
    public class SpellSlotReplenishmentViewModel : BaseViewModel
    {
        private bool _isOpen;
        private Action<IEnumerable<int>, IEnumerable<int>> _callback;

        public bool IsOpen
        {
            get
            {
                return _isOpen;
            }

            set
            {
                _isOpen = value;
                NotifyPropertyChanged();
            }
        }
    }
}