using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

using Xunit.Abstractions;
using Xunit.Sdk;

namespace Browser.xUnit.Sdk
{
    public class BrowserTestAssemblyRunner : XunitTestAssemblyRunner
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="T:Xunit.Sdk.XunitTestAssemblyRunner"/> class.
        /// </summary>
        /// <param name="testAssembly">The assembly that contains the tests to be run.</param><param name="testCases">The test cases to be run.</param><param name="diagnosticMessageSink">The message sink to report diagnostic messages to.</param><param name="executionMessageSink">The message sink to report run status to.</param><param name="executionOptions">The user's requested execution options.</param>
        public BrowserTestAssemblyRunner(ITestAssembly testAssembly, IEnumerable<IXunitTestCase> testCases, IMessageSink diagnosticMessageSink, IMessageSink executionMessageSink, ITestFrameworkExecutionOptions executionOptions)
            : base(testAssembly, testCases, diagnosticMessageSink, executionMessageSink, executionOptions)
        {
        }
        
        /// <inheritdoc/>
        protected override Task<RunSummary> RunTestCollectionAsync(IMessageBus messageBus, ITestCollection testCollection, IEnumerable<IXunitTestCase> testCases, CancellationTokenSource cancellationTokenSource)
            => new BrowserTestCollectionRunner(testCollection, testCases, DiagnosticMessageSink, messageBus, TestCaseOrderer, new ExceptionAggregator(Aggregator), cancellationTokenSource).RunAsync();


        /// <inheritdoc/>
        protected override Task AfterTestAssemblyStartingAsync()
        {
            // TODO: Add easy extensibility point for global setup.
            return base.AfterTestAssemblyStartingAsync();
        }

        /// <inheritdoc/>
        protected override Task BeforeTestAssemblyFinishedAsync()
        {
            // TODO: Add easy extensibility point for global teardown.
            return base.BeforeTestAssemblyFinishedAsync();
        }
    }
}