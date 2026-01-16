
using Microsoft.VisualStudio.TestTools.UnitTesting;

public class IntegrationTestMethodAttribute : TestMethodAttribute
{
    private readonly TestMethodAttribute _testMethodAttribute;

    public IntegrationTestMethodAttribute()
    {
    }

    public IntegrationTestMethodAttribute(TestMethodAttribute testMethodAttribute)
    {
        _testMethodAttribute = testMethodAttribute;
    }

    public override TestResult[] Execute(ITestMethod testMethod)
    {
        if (System.Diagnostics.Debugger.IsAttached)
        {
            return Invoke(testMethod);
        }
        return
            new TestResult[]
            {
                new TestResult() { Outcome = UnitTestOutcome.Passed }
            };


    }

    private TestResult[] Invoke(ITestMethod testMethod)
    {
        if (_testMethodAttribute != null)
            return _testMethodAttribute.Execute(testMethod);

        return new[] { testMethod.Invoke(null) };
    }
}