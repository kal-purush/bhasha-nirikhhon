using Xunit.Abstractions;
using Xunit.Sdk;

namespace Browser.xUnit.Sdk.Framework
{
    public class BrowserFactDiscoverer : FactDiscoverer
    {
        private readonly IMessageSink _diagnosticMessageSink;

        /// <summary>
        /// Initializes a new instance of the <see cref="T:Xunit.Sdk.FactDiscoverer"/> class.
        /// </summary>
        /// <param name="diagnosticMessageSink">The message sink used to send diagnostic messages</param>
        public BrowserFactDiscoverer(IMessageSink diagnosticMessageSink)
            : base(diagnosticMessageSink)
        {
            _diagnosticMessageSink = diagnosticMessageSink;
        }

        /// <summary>
        /// Creates a single <see cref="T:Xunit.Sdk.XunitTestCase"/> for the given test method.
        /// </summary>
        /// <param name="discoveryOptions">The discovery options to be used.</param><param name="testMethod">The test method.</param><param name="factAttribute">The attribute that decorates the test method.</param>
        /// <returns/>
        protected override IXunitTestCase CreateTestCase(
            ITestFrameworkDiscoveryOptions discoveryOptions,
            ITestMethod testMethod,
            IAttributeInfo factAttribute)
            => new BrowserTestCase(_diagnosticMessageSink, discoveryOptions.MethodDisplayOrDefault(), testMethod);
    }
}