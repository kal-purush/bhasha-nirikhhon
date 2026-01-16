namespace NetEvolve.Extensions.NUnit;

using global::NUnit.Framework;
using global::NUnit.Framework.Interfaces;
using System.Threading.Tasks;

/// <summary>
/// Basic implementation for the execution of continuous tests, based on <see cref="OrderAttribute"/>.
/// If a test fails, all subsequent tests are disabled.
/// </summary>
public abstract class ContinuousTestBase
{
    private bool _stopExecution;

    /// <summary>
    /// Setup method, that checks whether the triggered test should be executed or cancelled.
    /// </summary>
    [SetUp]
    public virtual Task SetUpAsync()
    {
        if (_stopExecution)
        {
            Assert.Inconclusive(
                "Previous test failed, further execution for this test class has been disabled!"
            );
        }

        return Task.CompletedTask;
    }

    /// <summary>
    /// Tear-down method, which determines whether the previous test was faulty. If <see langword="true"/>, the following tests are disabled.
    /// </summary>
    [TearDown]
    public virtual Task TearDownAsync()
    {
        if (TestContext.CurrentContext.Result.Outcome.Status == TestStatus.Failed)
        {
            _stopExecution = true;
        }

        return Task.CompletedTask;
    }
}