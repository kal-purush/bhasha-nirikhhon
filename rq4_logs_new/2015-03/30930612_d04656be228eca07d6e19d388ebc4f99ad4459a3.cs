using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using GW2Trader.ViewModel;

namespace GW2Trader.Command
{
    public class DeleteInvestmentListCommand : RelayCommand
    {
        public override bool CanExecute(object parameter)
        {
            InvestmentViewModel viewModel = parameter as InvestmentViewModel;
            return viewModel.SelectedWatchlist != null;
        }

        public override void Execute(object parameter)
        {
            throw new NotImplementedException();
        }
    }
}