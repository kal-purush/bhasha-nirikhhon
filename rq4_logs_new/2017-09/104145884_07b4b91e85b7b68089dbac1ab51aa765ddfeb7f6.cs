namespace HardySoft.GpsTracker.ViewModels
{
    using System;
    using System.Collections.Generic;
    using System.Collections.ObjectModel;
    using System.ComponentModel;
    using System.Linq;
    using System.Text;
    using System.Threading.Tasks;
    using System.Windows.Input;
    using HardySoft.GpsTracker.Models;
    using Prism.Commands;
    using Prism.Mvvm;
    using Windows.UI.Xaml.Controls;

    /// <summary>
    /// A view model class for main page.
    /// </summary>
    public class MainPageViewModel : BindableBase
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="MainPageViewModel"/> class.
        /// </summary>
        public MainPageViewModel()
        {
            this.NavigateCommand = new DelegateCommand<ItemClickEventArgs>(this.OnNavigate, this.CanNavigate);

            this.MainMenuItems = new ObservableCollection<MenuItem>(MenuItem.GetMainItems());
            this.OptionMenuItems = new ObservableCollection<MenuItem>(MenuItem.GetOptionsItems());
        }

        /// <summary>
        /// Gets the menu items used for top part of hamburger menu.
        /// </summary>
        public ObservableCollection<MenuItem> MainMenuItems { get; private set; }

        /// <summary>
        /// Gets the menu items for the bottom part of the hamburger menu.
        /// </summary>
        public ObservableCollection<MenuItem> OptionMenuItems { get; private set; }

        /// <summary>
        /// Gets the navigate command to other page.
        /// </summary>
        public ICommand NavigateCommand { get; private set; }

        /// <summary>
        /// Navigates to another page.
        /// </summary>
        /// <param name="argument">The page to navigate to.</param>
        private void OnNavigate(ItemClickEventArgs argument)
        {
            return;
        }

        /// <summary>
        /// Checks if the designated page can be navigated to.
        /// </summary>
        /// <param name="argument">The page to navigate to.</param>
        /// <returns>True if the navigation is allowed.</returns>
        private bool CanNavigate(ItemClickEventArgs argument)
        {
            return true;
        }
    }
}