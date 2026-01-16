using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net.NetworkInformation;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using System.Management;
using System.Linq;
using System.Net;

namespace AIPal.Services.AgentTools
{
    /// <summary>
    /// Provides tools for analyzing and securing Wi-Fi networks.
    /// Designed for elderly users with simple explanations and actionable advice.
    /// </summary>
    public static class NetworkSecurityTools
    {
        /// <summary>
        /// Gets the collection of network security tools.
        /// </summary>
        public static IEnumerable<Tool> Tools
        {
            get
            {
                return new[]
                {
                    new Tool
                    {
                        Name = "check_wifi_security",
                        Description = "Checks the security of the current Wi-Fi connection and provides recommendations",
                        Execute = CheckWiFiSecurity
                    },
                    new Tool
                    {
                        Name = "explain_wifi_security_terms",
                        Description = "Explains Wi-Fi security terms in simple language for elderly users",
                        Execute = ExplainWiFiSecurityTerms
                    },
                    new Tool
                    {
                        Name = "get_network_status",
                        Description = "Gets the current network status and connection information",
                        Execute = GetNetworkStatus
                    },
                    new Tool
                    {
                        Name = "troubleshoot_connection",
                        Description = "Troubleshoots common Wi-Fi connection issues",
                        Execute = TroubleshootConnection
                    }
                };
            }
        }
        
        /// <summary>
        /// Checks the security of the current Wi-Fi connection and provides recommendations.
        /// </summary>
        private static async Task<string> CheckWiFiSecurity(JsonElement parameters)
        {
            try
            {
                var wifiInfo = GetWiFiInformation();
                var securityAssessment = AssessWiFiSecurity(wifiInfo);
                
                var response = new StringBuilder();
                response.AppendLine("# Wi-Fi Security Check");
                response.AppendLine();
                
                // Current connection info
                response.AppendLine("## Your Current Connection");
                response.AppendLine($"- **Network Name**: {wifiInfo.SSID}");
                response.AppendLine($"- **Security Type**: {wifiInfo.SecurityType}");
                response.AppendLine($"- **Signal Strength**: {wifiInfo.SignalStrength}");
                response.AppendLine();
                
                // Security assessment
                response.AppendLine("## Security Assessment");
                response.AppendLine($"- **Overall Security**: {securityAssessment.OverallRating}");
                response.AppendLine($"- **Security Level**: {securityAssessment.SecurityLevel}");
                response.AppendLine();
                
                // Recommendations
                response.AppendLine("## Recommendations");
                foreach (var recommendation in securityAssessment.Recommendations)
                {
                    response.AppendLine($"- {recommendation}");
                }
                
                return response.ToString();
            }
            catch (Exception ex)
            {
                return $"I'm sorry, I couldn't check your Wi-Fi security. Error: {ex.Message}\n\nPlease make sure you're connected to a Wi-Fi network and try again.";
            }
        }
        
        /// <summary>
        /// Explains Wi-Fi security terms in simple language for elderly users.
        /// </summary>
        private static async Task<string> ExplainWiFiSecurityTerms(JsonElement parameters)
        {
            string term = "";
            if (parameters.TryGetProperty("term", out var termElement))
            {
                term = termElement.GetString().ToLower();
            }
            
            var explanation = GetSecurityTermExplanation(term);
            
            var response = new StringBuilder();
            
            if (!string.IsNullOrEmpty(term))
            {
                response.AppendLine($"# {explanation.Term}");
                response.AppendLine();
                response.AppendLine(explanation.SimpleExplanation);
                response.AppendLine();
                response.AppendLine("## What This Means For You");
                response.AppendLine(explanation.PracticalAdvice);
            }
            else
            {
                response.AppendLine("# Common Wi-Fi Security Terms");
                response.AppendLine();
                response.AppendLine("Here are some common Wi-Fi security terms explained in simple language:");
                response.AppendLine();
                
                var commonTerms = new[] { "wpa", "wpa2", "wpa3", "wep", "public wifi", "password" };
                foreach (var commonTerm in commonTerms)
                {
                    var commonExplanation = GetSecurityTermExplanation(commonTerm);
                    response.AppendLine($"## {commonExplanation.Term}");
                    response.AppendLine(commonExplanation.SimpleExplanation);
                    response.AppendLine();
                }
            }
            
            return response.ToString();
        }
        
        /// <summary>
        /// Gets the current network status and connection information.
        /// </summary>
        private static async Task<string> GetNetworkStatus(JsonElement parameters)
        {
            try
            {
                var networkStatus = CheckNetworkStatus();
                var wifiInfo = GetWiFiInformation();
                
                var response = new StringBuilder();
                response.AppendLine("# Network Status");
                response.AppendLine();
                
                // Connection status
                response.AppendLine("## Connection Status");
                response.AppendLine($"- **Internet Connection**: {(networkStatus.IsConnectedToInternet ? "Connected" : "Disconnected")}");
                response.AppendLine($"- **Connection Type**: {networkStatus.ConnectionType}");
                
                if (networkStatus.IsConnectedToInternet && networkStatus.ConnectionType == "Wi-Fi")
                {
                    // Wi-Fi details
                    response.AppendLine();
                    response.AppendLine("## Wi-Fi Details");
                    response.AppendLine($"- **Network Name**: {wifiInfo.SSID}");
                    response.AppendLine($"- **Signal Strength**: {wifiInfo.SignalStrength}");
                    response.AppendLine($"- **Security Type**: {wifiInfo.SecurityType}");
                    
                    // IP information
                    response.AppendLine();
                    response.AppendLine("## Network Information");
                    response.AppendLine($"- **IP Address**: {networkStatus.IPAddress}");
                    response.AppendLine($"- **MAC Address**: {networkStatus.MACAddress}");
                }
                
                return response.ToString();
            }
            catch (Exception ex)
            {
                return $"I'm sorry, I couldn't get your network status. Error: {ex.Message}";
            }
        }
        
        /// <summary>
        /// Troubleshoots common Wi-Fi connection issues.
        /// </summary>
        private static async Task<string> TroubleshootConnection(JsonElement parameters)
        {
            try
            {
                string issue = "";
                if (parameters.TryGetProperty("issue", out var issueElement))
                {
                    issue = issueElement.GetString().ToLower();
                }
                
                var networkStatus = CheckNetworkStatus();
                var troubleshootingSteps = GetTroubleshootingSteps(networkStatus, issue);
                
                var response = new StringBuilder();
                response.AppendLine("# Wi-Fi Troubleshooting");
                response.AppendLine();
                
                // Current status
                response.AppendLine("## Current Status");
                response.AppendLine($"- **Internet Connection**: {(networkStatus.IsConnectedToInternet ? "Connected" : "Disconnected")}");
                response.AppendLine($"- **Connection Type**: {networkStatus.ConnectionType}");
                response.AppendLine($"- **Detected Issue**: {troubleshootingSteps.DetectedIssue}");
                response.AppendLine();
                
                // Troubleshooting steps
                response.AppendLine("## Step-by-Step Solutions");
                for (int i = 0; i < troubleshootingSteps.Steps.Count; i++)
                {
                    response.AppendLine($"{i + 1}. {troubleshootingSteps.Steps[i]}");
                }
                
                return response.ToString();
            }
            catch (Exception ex)
            {
                return $"I'm sorry, I couldn't troubleshoot your connection. Error: {ex.Message}";
            }
        }
        
        #region Helper Methods
        
        /// <summary>
        /// Gets information about the current Wi-Fi connection.
        /// </summary>
        private static WiFiInformation GetWiFiInformation()
        {
            // In a real implementation, this would use Windows APIs to get actual Wi-Fi information
            // For this example, we'll return simulated data
            
            return new WiFiInformation
            {
                SSID = "HomeNetwork",
                SecurityType = "WPA2-Personal",
                SignalStrength = "Good (4/5)",
                IsSecured = true,
                EncryptionType = "AES"
            };
        }
        
        /// <summary>
        /// Assesses the security of a Wi-Fi connection.
        /// </summary>
        private static SecurityAssessment AssessWiFiSecurity(WiFiInformation wifiInfo)
        {
            var assessment = new SecurityAssessment();
            
            // Evaluate security based on encryption type
            if (wifiInfo.SecurityType.Contains("WPA3"))
            {
                assessment.SecurityLevel = "Strong";
                assessment.OverallRating = "Excellent";
                assessment.Recommendations.Add("Your connection is using the latest security standards. Great job!");
            }
            else if (wifiInfo.SecurityType.Contains("WPA2"))
            {
                assessment.SecurityLevel = "Good";
                assessment.OverallRating = "Good";
                assessment.Recommendations.Add("Your connection is secure, but consider upgrading to WPA3 if your router supports it.");
            }
            else if (wifiInfo.SecurityType.Contains("WPA"))
            {
                assessment.SecurityLevel = "Moderate";
                assessment.OverallRating = "Fair";
                assessment.Recommendations.Add("WPA is outdated. Consider upgrading your router to one that supports WPA2 or WPA3.");
            }
            else if (wifiInfo.SecurityType.Contains("WEP"))
            {
                assessment.SecurityLevel = "Weak";
                assessment.OverallRating = "Poor";
                assessment.Recommendations.Add("WEP is not secure. Please upgrade your router as soon as possible.");
                assessment.Recommendations.Add("Until you can upgrade, avoid sensitive activities like online banking on this network.");
            }
            else if (!wifiInfo.IsSecured)
            {
                assessment.SecurityLevel = "None";
                assessment.OverallRating = "Dangerous";
                assessment.Recommendations.Add("Your network has no security! Set up a password immediately.");
                assessment.Recommendations.Add("Avoid any sensitive activities on this network.");
            }
            
            // Add general recommendations
            assessment.Recommendations.Add("Make sure your Wi-Fi password is strong and unique.");
            assessment.Recommendations.Add("Don't share your Wi-Fi password with people you don't trust.");
            
            return assessment;
        }
        
        /// <summary>
        /// Gets an explanation for a Wi-Fi security term.
        /// </summary>
        private static SecurityTermExplanation GetSecurityTermExplanation(string term)
        {
            switch (term.ToLower())
            {
                case "wpa":
                    return new SecurityTermExplanation
                    {
                        Term = "WPA (Wi-Fi Protected Access)",
                        SimpleExplanation = "WPA is an older type of Wi-Fi security that protects your network with a password.",
                        PracticalAdvice = "If your router only supports WPA, it's time to consider getting a newer router. WPA is better than no security, but it's not as strong as newer options."
                    };
                    
                case "wpa2":
                    return new SecurityTermExplanation
                    {
                        Term = "WPA2 (Wi-Fi Protected Access 2)",
                        SimpleExplanation = "WPA2 is a common type of Wi-Fi security that provides good protection for your home network.",
                        PracticalAdvice = "WPA2 is secure for most home users. Make sure you use a strong, unique password that's at least 12 characters long."
                    };
                    
                case "wpa3":
                    return new SecurityTermExplanation
                    {
                        Term = "WPA3 (Wi-Fi Protected Access 3)",
                        SimpleExplanation = "WPA3 is the newest and most secure type of Wi-Fi protection available.",
                        PracticalAdvice = "If your router supports WPA3, you should use it. It provides the best protection against hackers trying to break into your network."
                    };
                    
                case "wep":
                    return new SecurityTermExplanation
                    {
                        Term = "WEP (Wired Equivalent Privacy)",
                        SimpleExplanation = "WEP is a very old and unsafe type of Wi-Fi security that can be easily broken by hackers.",
                        PracticalAdvice = "If your router is using WEP, you should replace it as soon as possible. WEP offers almost no real protection for your network."
                    };
                    
                case "public wifi":
                case "public":
                    return new SecurityTermExplanation
                    {
                        Term = "Public Wi-Fi",
                        SimpleExplanation = "Public Wi-Fi refers to free internet connections available in places like coffee shops, libraries, and airports. These networks are often unsecured or have minimal security.",
                        PracticalAdvice = "Be very careful when using public Wi-Fi. Don't do online banking, shopping, or anything that involves passwords or personal information. If you must use public Wi-Fi, consider using a VPN (Virtual Private Network) for protection."
                    };
                    
                case "password":
                case "wifi password":
                    return new SecurityTermExplanation
                    {
                        Term = "Wi-Fi Password",
                        SimpleExplanation = "A Wi-Fi password is the code you enter to connect to a secured wireless network. It prevents unauthorized people from using your internet connection.",
                        PracticalAdvice = "Use a strong, unique password for your Wi-Fi. It should be at least 12 characters long and include a mix of letters, numbers, and symbols. Don't use common words or personal information like birthdays."
                    };
                    
                default:
                    return new SecurityTermExplanation
                    {
                        Term = "Wi-Fi Security",
                        SimpleExplanation = "Wi-Fi security refers to the various methods used to protect your wireless network from unauthorized access and hackers.",
                        PracticalAdvice = "For the best protection, use WPA2 or WPA3 security with a strong password. Regularly update your router's firmware, and consider changing your Wi-Fi password every few months."
                    };
            }
        }
        
        /// <summary>
        /// Checks the current network status.
        /// </summary>
        private static NetworkStatus CheckNetworkStatus()
        {
            var status = new NetworkStatus();
            
            // Check internet connectivity
            try
            {
                using (var ping = new Ping())
                {
                    var reply = ping.Send("8.8.8.8", 3000);
                    status.IsConnectedToInternet = (reply != null && reply.Status == IPStatus.Success);
                }
            }
            catch
            {
                status.IsConnectedToInternet = false;
            }
            
            // In a real implementation, we would use NetworkInterface.GetAllNetworkInterfaces()
            // to get the actual network information
            // For this example, we'll return simulated data
            
            status.ConnectionType = "Wi-Fi";
            status.IPAddress = "192.168.1.100";
            status.MACAddress = "00:11:22:33:44:55";
            
            return status;
        }
        
        /// <summary>
        /// Gets troubleshooting steps for common Wi-Fi issues.
        /// </summary>
        private static TroubleshootingSteps GetTroubleshootingSteps(NetworkStatus status, string issue)
        {
            var steps = new TroubleshootingSteps();
            
            if (!status.IsConnectedToInternet)
            {
                steps.DetectedIssue = "No Internet Connection";
                steps.Steps.Add("Check if your Wi-Fi is turned on. Look for a Wi-Fi button or switch on your computer.");
                steps.Steps.Add("Make sure the Wi-Fi icon in the bottom-right corner of your screen shows you're connected to a network.");
                steps.Steps.Add("Restart your router by unplugging it, waiting 30 seconds, and plugging it back in.");
                steps.Steps.Add("Wait 2-3 minutes for the router to fully restart.");
                steps.Steps.Add("Try connecting to your Wi-Fi network again.");
            }
            else if (issue.Contains("slow") || issue.Contains("speed"))
            {
                steps.DetectedIssue = "Slow Connection";
                steps.Steps.Add("Move closer to your Wi-Fi router if possible.");
                steps.Steps.Add("Check if other devices in your home are using a lot of internet (like streaming videos or downloading large files).");
                steps.Steps.Add("Restart your router by unplugging it, waiting 30 seconds, and plugging it back in.");
                steps.Steps.Add("If the problem persists, contact your internet service provider to check if there are outages in your area.");
            }
            else if (issue.Contains("disconnect") || issue.Contains("dropping"))
            {
                steps.DetectedIssue = "Unstable Connection";
                steps.Steps.Add("Move closer to your Wi-Fi router if possible.");
                steps.Steps.Add("Check for sources of interference like microwave ovens or cordless phones near your computer or router.");
                steps.Steps.Add("Restart your router by unplugging it, waiting 30 seconds, and plugging it back in.");
                steps.Steps.Add("If you have an older router, consider upgrading to a newer model with better range.");
            }
            else if (issue.Contains("password") || issue.Contains("forgot"))
            {
                steps.DetectedIssue = "Password Issues";
                steps.Steps.Add("Check if your keyboard's Caps Lock is turned off, as Wi-Fi passwords are case-sensitive.");
                steps.Steps.Add("Try typing the password in a text editor first to make sure it's correct, then copy and paste it.");
                steps.Steps.Add("If you've forgotten your Wi-Fi password, look for it on the bottom of your router.");
                steps.Steps.Add("If you can't find your password, you may need to reset your router and set up a new password.");
            }
            else
            {
                steps.DetectedIssue = "General Wi-Fi Issues";
                steps.Steps.Add("Restart your computer and see if that resolves the issue.");
                steps.Steps.Add("Restart your router by unplugging it, waiting 30 seconds, and plugging it back in.");
                steps.Steps.Add("Check if other devices can connect to your Wi-Fi. If they can, the problem might be with your computer.");
                steps.Steps.Add("Make sure your Wi-Fi is turned on. Look for a Wi-Fi button or switch on your computer.");
                steps.Steps.Add("If nothing works, contact your internet service provider for help.");
            }
            
            return steps;
        }
        
        #endregion
        
        #region Data Models
        
        /// <summary>
        /// Represents information about a Wi-Fi connection.
        /// </summary>
        private class WiFiInformation
        {
            public string SSID { get; set; }
            public string SecurityType { get; set; }
            public string SignalStrength { get; set; }
            public bool IsSecured { get; set; }
            public string EncryptionType { get; set; }
        }
        
        /// <summary>
        /// Represents a security assessment of a Wi-Fi connection.
        /// </summary>
        private class SecurityAssessment
        {
            public string SecurityLevel { get; set; }
            public string OverallRating { get; set; }
            public List<string> Recommendations { get; set; } = new List<string>();
        }
        
        /// <summary>
        /// Represents an explanation of a Wi-Fi security term.
        /// </summary>
        private class SecurityTermExplanation
        {
            public string Term { get; set; }
            public string SimpleExplanation { get; set; }
            public string PracticalAdvice { get; set; }
        }
        
        /// <summary>
        /// Represents the current network status.
        /// </summary>
        private class NetworkStatus
        {
            public bool IsConnectedToInternet { get; set; }
            public string ConnectionType { get; set; }
            public string IPAddress { get; set; }
            public string MACAddress { get; set; }
        }
        
        /// <summary>
        /// Represents troubleshooting steps for Wi-Fi issues.
        /// </summary>
        private class TroubleshootingSteps
        {
            public string DetectedIssue { get; set; }
            public List<string> Steps { get; set; } = new List<string>();
        }
        
        #endregion
    }
}