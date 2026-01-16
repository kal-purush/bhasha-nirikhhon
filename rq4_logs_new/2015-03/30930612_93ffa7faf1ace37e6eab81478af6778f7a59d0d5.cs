using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GW2Trader.Model;
using GW2Trader.ViewModel;

namespace GW2Trader.Command
{
    public class AddWatchlistCommand : RelayCommand
    {
        public override bool CanExecute(object parameter)
        {
            return true;
        }

        public override void Execute(object parameter)
        {
            WatchlistViewModel viewModel = parameter as WatchlistViewModel;
            viewModel.AddWatchlist(new ItemWatchlistModel
            {
                Name = "new watchlist",
                Items = new List<GameItemModel>()
            });
            viewModel.SelectedWatchlist = viewModel.Watchlists.Last();
        }
    }
}