using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows;
using EntityFramework.MappingAPI.Exceptions;
using GW2Trader.View;
using GW2Trader.ViewModel;
using MahApps.Metro.Controls;
using MahApps.Metro.Controls.Dialogs;

namespace GW2Trader.Command
{
    public class AddInvestmentCommand : RelayCommand
    {
        public override bool CanExecute(object parameter)
        {
            InvestmentViewModel viewModel = parameter as InvestmentViewModel;
            return viewModel.SelectedWatchlist != null;
        }

        public override void Execute(object parameter)
        {
            //object[] parameters = parameter as object[];
            //MetroWindow mainWindow = parameters[1] as MetroWindow;
            InvestmentViewModel viewModel = parameter as InvestmentViewModel;
            
            NewInvestmentViewModel newInvestmentViewModel = new NewInvestmentViewModel(viewModel.SharedItems);

            NewInvestmentWindow newInvestmentWindow = new NewInvestmentWindow
            {
                DataContext = newInvestmentViewModel
            };
            newInvestmentWindow.ShowDialog();
        }
    }
}