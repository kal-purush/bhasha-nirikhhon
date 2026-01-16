using System;
using System.Collections.Generic;
using System.Linq;
using System.ServiceModel;
using System.ServiceModel.Channels;
using System.ServiceModel.Dispatcher;
using System.Text;
using System.Threading.Tasks;

namespace ServiceExample
{
    public class ErrorHandler : IErrorHandler
    {
        public bool HandleError(Exception error)
        {
            Console.WriteLine(error.Message);
            return true;
        }

        public void ProvideFault(Exception error, MessageVersion version, ref Message fault)
        {
            if (error is FaultException)
                return;

            var flt = new FaultException(error.Message, new FaultCode("InnerServiceException"));
            var msg = flt.CreateMessageFault();
            fault = Message.CreateMessage(version, msg, "null");
        }
    }
}