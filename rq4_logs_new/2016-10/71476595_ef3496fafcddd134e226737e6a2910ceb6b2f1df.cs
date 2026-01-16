using Prism.Events;
using Prism.Mef.Modularity;
using Prism.Modularity;
using Prism.Regions;
using System.ComponentModel.Composition;
using TplPlayground.CommandFileProcessor.View;
using TplPlayground.CommandFileProcessor.ViewModel;
using TplPlayground.Core;

namespace TplPlayground.CommandFileProcessor
{
    [ModuleExport(typeof(CommandFileProcessorModule))]
    public class CommandFileProcessorModule : IModule
    {
        private readonly IRegionManager _regionManager;
        private readonly IEventAggregator _eventAggregator;

        [ImportingConstructor]
        public CommandFileProcessorModule(IRegionManager regionManager, IEventAggregator eventAggregator)
        {
            this._regionManager = regionManager;
            this._eventAggregator = eventAggregator;
        }

        public void Initialize()
        {
            // add views to regions
            this._regionManager.RegisterViewWithRegion(RegionNames.ButtonRegion, typeof(NavigationButton));
            this._eventAggregator.GetEvent<NavigationEvent>().Subscribe(NavigateTo);
        }

        private void NavigateTo(string regionName) =>
            this._regionManager.RequestNavigate(regionName, MainContent.ContractName);
    }
}