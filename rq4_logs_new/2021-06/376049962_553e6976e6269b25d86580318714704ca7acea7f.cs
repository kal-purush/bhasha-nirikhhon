using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CurrencyConverter.ViewModels
{
    internal class MainWindowViewModel : ViewModel
    {
        private string _Title;

        public string Title 
        {
            get => _Title;

            set => Set(ref _Title, value);
        }
    }
}