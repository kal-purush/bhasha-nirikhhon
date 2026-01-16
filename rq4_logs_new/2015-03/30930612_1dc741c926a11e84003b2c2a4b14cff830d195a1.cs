using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GW2Trader.Model;
using GW2Trader.ViewModel;

namespace GW2Trader.Command
{
    public class AddItemsToWatchlistCommand : RelayCommand
    {
        public override bool CanExecute(object parameter)
        {
            return true;
        }

        public override void Execute(object parameter)
        {
            var parameters = parameter as object[];
            List<GameItemModel> selectedItems = (parameters[0] as IList).Cast<GameItemModel>().ToList();
            WatchlistViewModel viewModel = parameters[1] as WatchlistViewModel;
            ItemWatchlistModel watchlist = parameters[2] as ItemWatchlistModel;

            viewModel.AddItemsToWatchlist(selectedItems, watchlist);
        }
    }
}