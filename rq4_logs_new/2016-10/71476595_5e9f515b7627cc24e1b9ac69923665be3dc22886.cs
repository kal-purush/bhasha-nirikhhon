using Prism.Mef.Modularity;
using Prism.Modularity;
using Prism.Regions;
using System.ComponentModel.Composition;
using TplPlayground.Core;
using TplPlayground.EventViewerModule.View;

namespace TplPlayground.EventViewerModule
{
    [ModuleExport(typeof(EventViewerModule))]
    public class EventViewerModule : IModule
    {
        private readonly IRegionManager _regionManager;

        [ImportingConstructor]
        public EventViewerModule(IRegionManager regionManager)
        {
            this._regionManager = regionManager;
        }

        public void Initialize()
        {
            // add views to regions
            this._regionManager.RegisterViewWithRegion(RegionNames.LogRegion, typeof(EventLogView));
        }
    }
}