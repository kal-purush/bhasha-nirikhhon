// Scheduler.cs
using System;
using System.Threading.Tasks;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace InvoiceIntegration
{
    public static class Scheduler
    {
        // This function runs on a schedule (nightly at 1:00 AM by default)
        [FunctionName("InvoiceIntegrationScheduler")]
        public static async Task Run(
            [TimerTrigger("0 0 1 * * *")] TimerInfo myTimer, 
            ILogger log)
        {
            log.LogInformation($"Invoice integration process started at: {DateTime.Now}");
            
            try
            {
                // Step 1: Extract invoices from billing system
                log.LogInformation("Extracting invoices from billing system...");
                var invoices = await BillingSystemExtractor.ExtractInvoicesAsync();
                log.LogInformation($"Extracted {invoices.Count} invoices from billing system");
                
                // Step 2: Process each invoice
                int successCount = 0;
                int failureCount = 0;
                
                foreach (var invoice in invoices)
                {
                    try
                    {
                        // Transform invoice data to D365 format
                        var d365Invoice = DataTransformer.TransformInvoiceData(invoice);
                        
                        // Create invoice in D365
                        var result = await D365Connector.CreateInvoiceAsync(d365Invoice);
                        
                        // If successful, update billing system with D365 invoice number
                        if (result.Status == "SUCCESS")
                        {
                            await CallbackProcessor.UpdateBillingSystemAsync(invoice.InvoiceId, result.D365InvoiceId);
                            successCount++;
                        }
                        else
                        {
                            log.LogWarning($"Failed to create invoice in D365: {result.Message}");
                            failureCount++;
                        }
                    }
                    catch (Exception ex)
                    {
                        // Handle individual invoice processing errors
                        await ErrorHandler.HandleExceptionAsync(ex, JsonConvert.SerializeObject(invoice));
                        failureCount++;
                    }
                }
                
                // Step 3: Generate summary report
                log.LogInformation($"Invoice integration completed. Success: {successCount}, Failure: {failureCount}");
                
                // Step 4: Send notification if there were failures
                if (failureCount > 0)
                {
                    await SendFailureNotificationAsync(successCount, failureCount);
                }
            }
            catch (Exception ex)
            {
                // Handle overall process errors
                log.LogError($"Error in invoice integration process: {ex.Message}");
                await ErrorHandler.HandleExceptionAsync(ex, "Overall invoice integration process");
                
                // Send critical error notification
                await SendCriticalErrorNotificationAsync(ex);
            }
        }
        
        private static async Task SendFailureNotificationAsync(int successCount, int failureCount)
        {
            // In a real implementation, this would send an email or other notification
            // For demonstration, we'll just log it
            var logger = GetLogger();
            logger.LogWarning($"Invoice integration completed with failures. Success: {successCount}, Failure: {failureCount}");
            
            // Simulate sending notification
            await Task.Delay(100);
        }
        
        private static async Task SendCriticalErrorNotificationAsync(Exception ex)
        {
            // In a real implementation, this would send an email or other notification
            // For demonstration, we'll just log it
            var logger = GetLogger();
            logger.LogError($"Critical error in invoice integration process: {ex.Message}");
            
            // Simulate sending notification
            await Task.Delay(100);
        }
        
        // In a real implementation, this would be a proper DI-based logger
        private static ILogger GetLogger()
        {
            var loggerFactory = LoggerFactory.Create(builder =>
            {
                builder.AddConsole();
            });
            
            return loggerFactory.CreateLogger("Scheduler");
        }
    }
}