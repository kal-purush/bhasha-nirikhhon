using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Web.Http;
using FAMS_AngularApi.Models.PMSCustomerList;
using System.Threading.Tasks;

namespace FAMS_AngularApi.Controllers
{
    public class PMSCustomerListController : ApiController
    {
        // GET: PMSCustomerList
        //public ActionResult Index()
        //{
        //    return View();
        //}


        PMSCustomerListDataAccessLayer objPMSCustomerList = new PMSCustomerListDataAccessLayer();
        [HttpGet]
        [Route("api/PMSCustomerList/BindCustodian")]
        public Dictionary<string, object> BindCustodian()
        {
            return objPMSCustomerList.BindCustodian();
        }

        [HttpGet]
        [Route("api/PMSCustomerList/BindPortfolio")]
        public Dictionary<string, object> BindPortfolio()
        {
            return objPMSCustomerList.BindPortfolio();
        }

        [HttpGet]
        [Route("api/PMSCustomerList/BindLinkedPMSEmployee")]
        public Dictionary<string, object> BindLinkedPMSEmployee()
        {
            return objPMSCustomerList.BindLinkedPMSEmployee();
        }

        [HttpGet]
        [Route("api/PMSCustomerList/BindPMSCustomerListDetails")]
        public Dictionary<string, object> BindPMSCustomerListDetails()
        {
            return objPMSCustomerList.BindPMSCustomerListDetails();
        }

        [HttpGet]
        [Route("api/PMSCustomerList/BindPMSCustomerListCodeDetails/{CustomerListId}")]
        public Dictionary<string, object> BindPMSCustomerListCodeDetails(string CustomerListId)
        {
            return objPMSCustomerList.BindPMSCustomerListCodeDetails(CustomerListId);
        }

        [HttpPost]
        [Route("api/PMSCustomerList/AddCustomerListDetails/{UserId}")]
        public IEnumerable<PMSCustomerList> AddCustomerListDetails([FromBody] PMSCustomerList pMSCustomerList, string UserId)
        {
            return objPMSCustomerList.AddCustomerListDetails(pMSCustomerList, UserId);
        }

        [HttpPost]
        [Route("api/PMSCustomerList/UpdateCustomerListDetails/{UserId}/{CustomerListId}")]
        public IEnumerable<PMSCustomerList> UpdateCustomerListDetails([FromBody] PMSCustomerList pMSCustomerList, string UserId, string CustomerListId)
        {
            return objPMSCustomerList.UpdateCustomerListDetails(pMSCustomerList, UserId, CustomerListId);
        }
    }
}