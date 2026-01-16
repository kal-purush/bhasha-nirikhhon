using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GW2Trader.Model;
using GW2Trader.View;
using GW2Trader.ViewModel;

namespace GW2Trader.Command
{
    public class DeleteWatchlistItemCommand : RelayCommand
    {
        public override bool CanExecute(object parameter)
        {
            return true;
        }

        public override void Execute(object parameter)
        {
            object[] parameters = parameter as object[];
            WatchlistViewModel viewModel = parameters[0] as WatchlistViewModel;
            GameItemModel item = parameters[1] as GameItemModel;
            viewModel.DeleteWatchlistItem(item);
        }
    }
}