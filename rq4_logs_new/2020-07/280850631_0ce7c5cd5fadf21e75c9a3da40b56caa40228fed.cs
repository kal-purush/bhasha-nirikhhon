using Covid19TemperatureAPI.Entities.Data;
using Microsoft.Extensions.Configuration;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Covid19TemperatureAPI.Helper
{
    public static class ConfigReader
    {
        const string DefaultTemperatureThresholdConfig = "DefaultTemperatureThreshold";
        const string DefaultTemperatureAlertHeaderConfig = "DefaultTemperatureAlertHeader";
        const string DefaultSendAlertSMSConfig = "DefaultSendAlertSMSEnabled";
        const string DefaultSMSSenderConfig = "DefaultSMSSender";

        public const string VisitorUID = "0";
        public const int NoMaskValue = 2;

        public static decimal GetTemperatureThreshold(ApplicationDbContext DbContext, IConfiguration Configuration)
        {
            bool res = decimal.TryParse(DbContext.Configurations
                   .Where(x => x.ConfigKey == "TemperatureThreshold")
                   .Select(x => x.ConfigValue).FirstOrDefault(), out decimal thresholdTemperature);

            if (!res)
                decimal.TryParse(Configuration[DefaultTemperatureThresholdConfig], out thresholdTemperature);

            return thresholdTemperature;
        }

        public static string GetTemperatureAlertHeader(ApplicationDbContext DbContext, IConfiguration Configuration)
        {
            string res = DbContext.Configurations
                   .Where(x => x.ConfigKey == "TemperatureAlertHeader")
                   .Select(x => x.ConfigValue).FirstOrDefault();

            if (string.IsNullOrEmpty(res))
                res = Configuration[DefaultTemperatureAlertHeaderConfig];

            return res;
        }

        public static bool GetSendAlertSMSEnabled(ApplicationDbContext DbContext, IConfiguration Configuration)
        {
            string res = DbContext.Configurations
                   .Where(x => x.ConfigKey == "SendAlertSMS")
                   .Select(x => x.ConfigValue).FirstOrDefault();

            if (string.IsNullOrEmpty(res))
            {
                res = Configuration[DefaultSendAlertSMSConfig];
            }

            if (res == "0") return false;
            if (res == "1") return true;

            return false;
        }

        public static string GetSMSSender(ApplicationDbContext DbContext, IConfiguration Configuration)
        {
            string res = DbContext.Configurations
                   .Where(x => x.ConfigKey == "SMSSender")
                   .Select(x => x.ConfigValue).FirstOrDefault();

            if (string.IsNullOrEmpty(res))
                res = Configuration[DefaultSMSSenderConfig];

            return res;
        }

    }
}