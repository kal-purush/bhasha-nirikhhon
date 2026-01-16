using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using OpenQA.Selenium;
using Polly;
using System;

namespace LegalLead.PublicData.Search.Extensions
{
    internal static class JavaScriptExecutorExtensions
    {
        /// <summary>
        /// Executes JavaScript with a custom timeout and retry policy.
        /// </summary>
        /// <param name="driver">The WebDriver instance.</param>
        /// <param name="executor">The JavaScript executor instance.</param>
        /// <param name="timeout">The custom timeout for the HTTP client.</param>
        /// <param name="script">The JavaScript to execute.</param>
        /// <returns>The result of the JavaScript execution.</returns>
        /// <exception cref="WebDriverException">Thrown when the execution fails after retries.</exception>
        /// <remarks>
        /// This method sets a custom HTTP client timeout and retries the JavaScript execution up to 5 times
        /// with exponential backoff intervals (1, 2, 4, 8, and 16 seconds) in case of exceptions.
        /// The original page-load timeout is restored after the method completes.
        /// </remarks>
        public static object ExecuteScriptWithRetry(this IWebDriver driver, IJavaScriptExecutor executor, TimeSpan timeout, string script)
        {
            var policy = Policy
                .Handle<WebDriverException>()
                .WaitAndRetry(5, retryAttempt =>
                {
                    var ms = 500 * Math.Pow(2, retryAttempt);
                    return TimeSpan.FromMilliseconds(ms);
                });

            object result = null;

            var originalTimeout = driver.Manage().Timeouts().PageLoad;

            try
            {
                policy.Execute(() =>
                {
                    driver.Manage().Timeouts().PageLoad = timeout;
                    result = executor.ExecuteScript(script);
                });
            }
            finally
            {
                // Restore the original page-load timeout
                driver.Manage().Timeouts().PageLoad = originalTimeout;
            }

            return result;
        }
    }
}