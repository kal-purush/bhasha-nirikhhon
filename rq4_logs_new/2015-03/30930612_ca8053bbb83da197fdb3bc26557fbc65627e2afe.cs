using GW2Trader.ViewModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace GW2Trader.Command
{
    public class UpdatePriceCommand : RelayCommand
    {
        public override bool CanExecute(object parameter)
        {
            return true;
        }

        public override void Execute(object parameter)
        {
            ItemSearchViewModel viewModel = parameter as ItemSearchViewModel;
            viewModel.UpdateCommerceData();
        }
    }
}