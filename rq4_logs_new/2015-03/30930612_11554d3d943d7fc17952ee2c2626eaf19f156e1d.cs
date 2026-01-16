using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GW2TPApiWrapper.Wrapper;
using GW2Trader.MVVM;
using GW2Trader.Data;
using GW2Trader.Model;
using GW2Trader.Command;
using System.Windows.Data;
using System.Windows.Input;
using System.ComponentModel;

namespace GW2Trader.ViewModel
{
    public class WatchlistViewModel : BaseViewModel
    {
        

        public WatchlistViewModel()
        {
            ViewModelName = "Watchlists";
        }
    }
}