using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GW2Trader.ViewModel;

namespace GW2Trader.Command
{
    public class UpdateCommerceDataCommand : RelayCommand
    {
        public override bool CanExecute(object parameter)
        {
            return true;
        }

        public override void Execute(object parameter)
        {
            MainViewModel viewModel = parameter as MainViewModel;
            viewModel.UpdateCommerceData();
        }
    }
}