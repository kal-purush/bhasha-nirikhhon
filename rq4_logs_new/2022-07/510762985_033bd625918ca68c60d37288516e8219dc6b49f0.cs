using TypewiseAlert.Common;
using TypewiseAlert.Enums;

namespace TypewiseAlert
{
    public class ControllerAlerter : IAlerter
    {
        public void SendAlert(BreachType breachType)
        {
            const ushort header = 0xfeed;
            Logger.LogMessage($"{header}:{breachType}");
        }

    }
}