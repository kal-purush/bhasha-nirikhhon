using System;
using System.Threading.Tasks;
using AIPal.Application.Security.Services;

namespace AIPal.Application.Commands
{
    /// <summary>
    /// Command to check system security.
    /// </summary>
    public class CheckSystemSecurityCommand : ICommand<string>
    {
        private readonly SecurityMonitorService _securityMonitorService;
        
        /// <summary>
        /// Initializes a new instance of the <see cref="CheckSystemSecurityCommand"/> class.
        /// </summary>
        /// <param name="securityMonitorService">The security monitor service.</param>
        public CheckSystemSecurityCommand(SecurityMonitorService securityMonitorService)
        {
            _securityMonitorService = securityMonitorService ?? throw new ArgumentNullException(nameof(securityMonitorService));
        }
        
        /// <summary>
        /// Executes the command to check system security.
        /// </summary>
        /// <returns>A string containing the system security assessment.</returns>
        public async Task<string> ExecuteAsync()
        {
            // Get security status
            var securityStatus = _securityMonitorService.GetSecurityStatus();
            
            // Get security recommendations
            var recommendations = _securityMonitorService.GetSecurityRecommendations();
            
            // Build a user-friendly response
            var response = new System.Text.StringBuilder();
            
            // Add overall security rating
            response.AppendLine("📊 System Security Assessment");
            response.AppendLine("---------------------------");
            
            switch (securityStatus.SecurityRating)
            {
                case SecurityRating.Good:
                    response.AppendLine("✅ Your system security is GOOD. Your computer is well-protected.");
                    break;
                case SecurityRating.Fair:
                    response.AppendLine("⚠️ Your system security is FAIR. There are some improvements you could make.");
                    break;
                case SecurityRating.Poor:
                    response.AppendLine("❌ Your system security is POOR. Your computer may be at risk.");
                    break;
            }
            
            response.AppendLine();
            
            // Add security status details
            response.AppendLine("🔍 Security Details:");
            response.AppendLine($"• Windows Defender: {(securityStatus.IsWindowsDefenderEnabled ? "Enabled ✅" : "Disabled ❌")}");
            response.AppendLine($"• Real-time Protection: {(securityStatus.IsRealtimeProtectionEnabled ? "Enabled ✅" : "Disabled ❌")}");
            
            if (securityStatus.LastDefinitionUpdateTime.HasValue)
            {
                var daysSinceUpdate = (DateTime.Now - securityStatus.LastDefinitionUpdateTime.Value).TotalDays;
                string updateStatus = daysSinceUpdate <= 7 ? "Recent ✅" : "Outdated ⚠️";
                response.AppendLine($"• Virus Definitions: {updateStatus} (Last updated {securityStatus.LastDefinitionUpdateTime.Value.ToShortDateString()})");
            }
            else
            {
                response.AppendLine("• Virus Definitions: Unknown ⚠️");
            }
            
            if (securityStatus.LastScanTime.HasValue)
            {
                var daysSinceScan = (DateTime.Now - securityStatus.LastScanTime.Value).TotalDays;
                string scanStatus = daysSinceScan <= 7 ? "Recent ✅" : "Outdated ⚠️";
                response.AppendLine($"• Last Virus Scan: {scanStatus} (Last scan {securityStatus.LastScanTime.Value.ToShortDateString()})");
            }
            else
            {
                response.AppendLine("• Last Virus Scan: Unknown ⚠️");
            }
            
            response.AppendLine($"• Windows Update: {(securityStatus.IsWindowsUpdateEnabled ? "Enabled ✅" : "Disabled ❌")}");
            
            response.AppendLine();
            
            // Add top recommendations (limit to 3 for readability)
            if (recommendations.Count > 0)
            {
                response.AppendLine("🛡️ Top Security Recommendations:");
                
                // Sort recommendations by priority
                recommendations.Sort((a, b) => b.Priority.CompareTo(a.Priority));
                
                // Show top 3 recommendations
                for (int i = 0; i < Math.Min(3, recommendations.Count); i++)
                {
                    var recommendation = recommendations[i];
                    response.AppendLine();
                    response.AppendLine($"{i + 1}. {recommendation.Title}");
                    response.AppendLine($"   {recommendation.Description}");
                    
                    response.AppendLine("   How to fix:");
                    foreach (var step in recommendation.Steps)
                    {
                        response.AppendLine($"   • {step}");
                    }
                }
            }
            
            return response.ToString();
        }
    }
}