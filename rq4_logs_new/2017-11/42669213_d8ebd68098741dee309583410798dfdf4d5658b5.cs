using CTime2.Core.Data;
using Microsoft.AppCenter;
using UwCore.Services.Analytics;

namespace CTime2.Extensions
{
    public static class AnalyticsServiceExtensions
    {
        public static void UpdateContactInfo(this IAnalyticsService self, User currentUser)
        {
            if (self is AppCenterAnalyticsService == false)
                return;

            var properties = new CustomProperties();
            if (currentUser == null)
            {
                properties.Clear("Name");
                properties.Clear("Email");
            }
            else
            {
                properties.Set("Name", $"{currentUser.FirstName} {currentUser.Name}");
                properties.Set("Email", currentUser.Email);
            }

            AppCenter.SetCustomProperties(properties);
        }
    }
}