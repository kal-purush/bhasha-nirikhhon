using FundSearcher.Consts;
using Prism.Regions;

namespace FundSearcher
{
    class BaseFundViewModel : BaseViewModel
    {
        public BaseFundViewModel(IRegionManager regionManager) : base(regionManager, RegionName.Fund)
        {
        }
    }
}