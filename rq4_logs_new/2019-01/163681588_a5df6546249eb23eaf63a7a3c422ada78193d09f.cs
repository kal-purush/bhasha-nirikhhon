using System;
using System.Collections.Generic;
using System.Data;
using Monitor.Interfaces;
using OpenQA.Selenium;
namespace Monitor.Providers
{
    class ttlValidator : Validator
    {
        public ttlValidator()
        {
        }

        public override void validate(IWebDriver driver, string url, string title = null)
        {
            throw new System.NotImplementedException();
        }

        public override void validate(IWebDriver driver, DataRow siteData)
        {
            TimeSpan ttl = TimeSpan.Parse(siteData["ttl"].ToString());
            TimeSpan requiredttl = TimeSpan.FromSeconds(siteData[3] is Double ? (Double)siteData[3] : Double.Parse(siteData[3].ToString()));
            if (ttl > requiredttl)
                notify($"Load time exeeded the expected time, desired: {requiredttl}, actual: {ttl}");
        }
    }
}