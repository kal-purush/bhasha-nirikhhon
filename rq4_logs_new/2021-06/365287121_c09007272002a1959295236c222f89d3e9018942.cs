using Arc.HttpHealthCheckDashboard;
using ArnabDeveloper.HttpHealthCheck;
using System.Collections.Generic;

namespace HttpHealthCheckDashboard
{
    public class GmailHealthCheck : BaseHealthCheck
    {
        public GmailHealthCheck(IEnumerable<ApiDetail> urlDetails, ICommonHealthCheck commonHealthCheck)
            : base(urlDetails, commonHealthCheck)
        {
        }
    }
}