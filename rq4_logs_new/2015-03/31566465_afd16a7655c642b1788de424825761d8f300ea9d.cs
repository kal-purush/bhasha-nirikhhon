using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;
using Microsoft.Crm.Sdk.Messages;
using Microsoft.Xrm.Client;
using Microsoft.Xrm.Client.Services;
using Microsoft.Xrm.Sdk;


namespace DiscoverWebService
{
   
    [WebService(Namespace = "http://tempuri.org/")]
    [WebServiceBinding(ConformsTo = WsiProfiles.BasicProfile1_1)]
    [System.ComponentModel.ToolboxItem(false)]

    public class DiscoverService : System.Web.Services.WebService
    {

        private IOrganizationService Connect()
        {
            CrmConnection con = new CrmConnection("CRM");
            IOrganizationService service = new OrganizationService(con);
            return service;
        }
        [WebMethod]
        public string HelloWorld()
        {
            var response = Connect();
            response.Execute(new WhoAmIRequest());
            return "Hello Discover";
            //return response.ToString;
        }
    }
}