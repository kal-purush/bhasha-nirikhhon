using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace HueLamp
{
    class MainViewModel
    {
        private ObservableCollection<String> _items;

        public MainViewModel()
        {
            _items = new ObservableCollection<String>();
            _items.Add("Watsup");
            _items.Add("Watsup2");
            _items.Add("Watsup3");
            _items.Add("yo");
            _items.Add("yo2");
            _items.Add("yo3");
            _items.Add("yo4");
            _items.Add("yo5");
            _items.Add("yo6");

            _items.Add("");

        }

        public ObservableCollection<String> Items
        {
            get { return _items; }
        }
    }
}