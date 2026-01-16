using System.Linq;
using System.Web.Mvc;
using LtInfo.Common.DesignByContract;
using LtInfo.Common.MvcResults;
using ProjectFirma.Web.Common;
using ProjectFirma.Web.Models;
using ProjectFirma.Web.Security;
using ProjectFirma.Web.Views.Obligation;
using ProjectFirma.Web.Views.ObligationItem;
using ProjectFirmaModels.Models;

namespace ProjectFirma.Web.Controllers
{
    public class ObligationItemController : FirmaBaseController
    {

        [ObligationItemViewFeature]
        public ViewResult ObligationItemIndex()
        {
            var firmaPage = FirmaPageTypeEnum.ObligationItemList.GetFirmaPage();
            var viewData = new ObligationItemIndexViewData(CurrentFirmaSession, firmaPage);
            return RazorView<ObligationItemIndex, ObligationItemIndexViewData>(viewData);
        }



        [ObligationItemViewFeature]
        public GridJsonNetJObjectResult<ObligationItem> ObligationItemsForObligationNumberGridJsonData(ObligationNumberPrimaryKey obligationNumberPrimaryKey)
        {
            var gridSpec = new ObligationItemGridSpec(CurrentFirmaSession);
            var obligationNumber = obligationNumberPrimaryKey.EntityObject;
            var obligationItems = obligationNumber.ObligationItems.ToList();
            var gridJsonNetJObjectResult = new GridJsonNetJObjectResult<ObligationItem>(obligationItems, gridSpec);
            return gridJsonNetJObjectResult;
        }

        [ObligationItemViewFeature]
        public GridJsonNetJObjectResult<ObligationItem> ObligationItemGridJsonData()
        {
            var gridSpec = new ObligationItemGridSpec(CurrentFirmaSession);
            var obligationItems = HttpRequestStorage.DatabaseEntities.ObligationItems.ToList();
            var gridJsonNetJObjectResult = new GridJsonNetJObjectResult<ObligationItem>(obligationItems, gridSpec);
            return gridJsonNetJObjectResult;
        }


    }
}