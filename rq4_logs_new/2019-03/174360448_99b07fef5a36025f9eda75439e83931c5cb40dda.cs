using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Text;

namespace APILayer.Client.Base
{
    public class ServiceRestApiClientBase
    {
        protected readonly string fromDateBaseAttr = "todate=";

        protected readonly string toDateBaseAttr = "todate=";


        protected readonly IConfigurationRoot configurationRoot;

        //Inject configuration json file into rest service base
        public ServiceRestApiClientBase(IConfigurationRoot configurationRoot)
        {
            this.configurationRoot = configurationRoot;
        }

        //authenticiy management if needed

        //swagger configs...
    }
}