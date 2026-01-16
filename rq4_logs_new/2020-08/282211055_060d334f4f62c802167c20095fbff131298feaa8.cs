using Desktop.Extensions.Helpers;
using Desktop.Interfaces;
using Desktop.Models;
using GalaSoft.MvvmLight;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Windows.Input;

namespace Desktop.ViewModels
{
  public class ApplicationViewModel : ViewModelBase
  {
    #region Members

    private ICommand _changePageCommand;

    private IPageViewModel _currentPageViewModel;
    private List<IPageViewModel> _pageViewModels;
    private bool _showHamburgerMenu = false;

    #endregion // Members

    public ApplicationViewModel()
    {
      // Add available pages
      PageViewModels.Add(new GameListViewModel(new GameList()));
      PageViewModels.Add(new StatsViewModel());

      // Set starting page
      CurrentPageViewModel = PageViewModels[0];
    }

    #region Properties

    public List<IPageViewModel> PageViewModels
    {
      get
      {
        if (_pageViewModels == null)
          _pageViewModels = new List<IPageViewModel>();

        return _pageViewModels;
      }
    }

    public IPageViewModel CurrentPageViewModel
    {
      get
      {
        return _currentPageViewModel;
      }
      set
      {
        if (_currentPageViewModel != value)
        {
          _currentPageViewModel = value;
          RaisePropertyChanged("CurrentPageViewModel");
        }
      }
    }

    public bool ShowHamburgerMenu { get { return _showHamburgerMenu; } set { _showHamburgerMenu = value; RaisePropertyChanged("ShowHamburgerMenu"); } }

    #endregion // Properties

    #region Commands

    /// <summary>
    /// Command to change to the view of the window
    /// </summary>
    public ICommand ChangePageCommand
    {
      get
      {
        if (_changePageCommand == null)
        {
          _changePageCommand = new RelayCommand(
              p => ChangeViewModel((IPageViewModel)p),
              p => p is IPageViewModel);
        }

        return _changePageCommand;
      }
    }

    #endregion // Commands

    #region Implementation

    /// <summary>
    /// Change the ViewModel to the given type, changing the View
    /// </summary>
    /// <param name="viewModel">View model to change to</param>
    private void ChangeViewModel(IPageViewModel viewModel)
    {
      if (!PageViewModels.Contains(viewModel))
      {
        PageViewModels.Add(viewModel);
      }

      CurrentPageViewModel = PageViewModels
          .FirstOrDefault(vm => vm == viewModel);

      ShowHamburgerMenu = false;
    } // ChangeViewModel

    #endregion // Implementation
  }
}