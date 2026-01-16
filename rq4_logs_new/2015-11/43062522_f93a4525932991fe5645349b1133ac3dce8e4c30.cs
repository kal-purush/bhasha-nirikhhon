using Saab.CBRN.Wcf.DataContracts;
using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.ServiceModel.Web;
using System.Text;

namespace Saab.CBRN.Wcf.ServiceContracts
{
    public partial interface IService
    {
        [OperationContract]
        [WebInvoke(Method = "POST", UriTemplate = "sensors/ap2ce", ResponseFormat = WebMessageFormat.Json)]
        void CreateAP2Ce(AP2Ce ap2ce);

        [OperationContract]
        [WebInvoke(Method = "GET", UriTemplate = "sensors/ap2ce/{id}", ResponseFormat = WebMessageFormat.Json)]
        AP2Ce GetAP2CeById(string id);

        [OperationContract]
        [WebInvoke(Method = "PUT", UriTemplate = "sensors/ap2ce", ResponseFormat = WebMessageFormat.Json)]
        void UpdateAP2Ce(AP2Ce ap2ce);

        [OperationContract]
        [WebInvoke(Method = "DELETE", UriTemplate = "sensors/ap2ce/{id}", ResponseFormat = WebMessageFormat.Json)]
        void DeleteAP2Ce(string id);
    }
}