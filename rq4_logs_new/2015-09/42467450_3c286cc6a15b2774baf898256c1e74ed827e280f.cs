
using System;

using Xunit.Abstractions;
using Xunit.Sdk;

namespace Browser.xUnit.Sdk
{
    public class BrowserTestFrameworkTypeDiscoverer : ITestFrameworkTypeDiscoverer
    {
        #region Implementation of ITestFrameworkTypeDiscoverer

        /// <summary>
        /// Gets the type that implements <see cref="T:Xunit.Abstractions.ITestFramework"/> to be used to discover
        ///             and run tests.
        /// </summary>
        /// <param name="attribute">The test framework attribute that decorated the assembly</param>
        /// <returns>
        /// The test framework type
        /// </returns>
        public Type GetTestFrameworkType(IAttributeInfo attribute)
        {
            return typeof(BrowserTestFramework);
        }

        #endregion
    }
}