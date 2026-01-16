using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Serialization;
using System.ServiceModel;
using System.ServiceModel.Web;
using System.Text;
using Shared;

namespace WebApplication1.App_Code
{
    // NOTE: You can use the "Rename" command on the "Refactor" menu to change the interface name "IService" in both code and config file together.
    
    public interface IService
    {

        [OperationContract]
        [WebInvoke]
        int BuyBook(string title, string name, string email, string address, int quantity);

        [OperationContract]
        [WebGet]
        Order OrderDetails(int orderId);

        [OperationContract]
        [WebGet]
        Book BookDetails(string title);

    }
}