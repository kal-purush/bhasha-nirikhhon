using H.Avalonia.Views.FarmCreationViews;
using Prism.Commands;
using Prism.Regions;
using System.Windows.Input;

namespace H.Avalonia.ViewModels
{
    public class FarmOpenExistingViewmodel : ViewModelBase
    {
        #region Fields
        private readonly IRegionManager _regionManager;
        #endregion

        #region Constructors
        public FarmOpenExistingViewmodel(IRegionManager regionManager) : base(regionManager)
        {
            _regionManager = regionManager ?? throw new System.ArgumentNullException(nameof(regionManager));
            NavigateToPreviousPage = new DelegateCommand(onNavigateToPreviousPage);
        }
        #endregion

        #region Properties
        public ICommand NavigateToPreviousPage { get; }
        #endregion

        #region Methods
        private void onNavigateToPreviousPage()
        {

            _regionManager.RequestNavigate(UiRegions.ContentRegion, nameof(FarmOptionsView));
        }
    }
    #endregion
}