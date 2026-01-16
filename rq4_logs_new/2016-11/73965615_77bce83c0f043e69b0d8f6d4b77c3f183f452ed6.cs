namespace VolumeControllerService.Utility
{
    using NetFwTypeLib;
    using System;
    using Values;

    public class FireWall
    {
        public static void CreateException(string appLocation)
        {
            Type tNetFwPolicy2 = Type.GetTypeFromProgID("HNetCfg.FwPolicy2");
            INetFwPolicy2 fwPolicy2 = (INetFwPolicy2)Activator.CreateInstance(tNetFwPolicy2);
            var currentProfiles = fwPolicy2.CurrentProfileTypes;

            foreach (var rule in fwPolicy2.Rules)
            {
                dynamic tempRule = rule;
                if (tempRule?.Name == ApplicationData.ServiceName)
                    return;
            }

            INetFwRule2 inboundRule = (INetFwRule2)Activator.CreateInstance(Type.GetTypeFromProgID("HNetCfg.FWRule"));
            inboundRule.Enabled = true;
            inboundRule.Action = NET_FW_ACTION_.NET_FW_ACTION_ALLOW;
            inboundRule.Protocol = 6;
            inboundRule.LocalPorts = URLDetails.Port;
            inboundRule.Name = ApplicationData.ServiceName;
            inboundRule.ApplicationName = appLocation;
            inboundRule.serviceName = ApplicationData.ServiceName;

            inboundRule.Profiles = currentProfiles;

            // Now add the rule
            INetFwPolicy2 firewallPolicy = (INetFwPolicy2)Activator.CreateInstance(Type.GetTypeFromProgID("HNetCfg.FwPolicy2"));
            firewallPolicy.Rules.Add(inboundRule);
        }
    }
}