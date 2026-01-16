using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GW2Trader.Model;
using GW2Trader.ViewModel;

namespace GW2Trader.Command
{
    public class UpdateListingsCommand : RelayCommand
    {
        public override bool CanExecute(object parameter)
        {
            return true;
        }

        public override void Execute(object parameter)
        {
            object[] parameters = parameter as object[];
            GameItemModel itemToUpdate = parameters[0] as GameItemModel;
            var viewModel = (ItemSearchViewModel)parameters[1];
            viewModel.UpdateListings(itemToUpdate);
        }
    }
}