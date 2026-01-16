using System;
using System.Collections.Generic;
using System.Text;
using Flee.PublicTypes;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Flee.Test.ExpressionTests
{
    [TestClass]
    public class BulkTests: Infrastructure.ExpressionTests
    {

    [TestMethod]
    public void TestValidExpressions()
    {
        MyCurrentContext = MyGenericContext;

        MyCurrentContext.Variables.ResolveFunction += TestValidExpressions_OnResolveFunction;
        MyCurrentContext.Variables.InvokeFunction += TestValidExpressions_OnInvokeFunction;

        //this.ProcessScriptTests("ValidExpressions.txt", DoTestValidExpressions);
        this.ProcessLine("String;DateTimeA.GetType().Name;DateTime", DoTestValidExpressions);

        MyCurrentContext.Variables.ResolveFunction -= TestValidExpressions_OnResolveFunction;
        MyCurrentContext.Variables.InvokeFunction -= TestValidExpressions_OnInvokeFunction;
    }

    public static object Ev(object owner)
    {
        return ((ExpressionOwner) owner).DateTimeA.GetType().Name;
    }

    private void TestValidExpressions_OnResolveFunction(object sender, ResolveFunctionEventArgs e)
    {
        e.ReturnType = typeof(int);
    }

    private void TestValidExpressions_OnInvokeFunction(object sender, InvokeFunctionEventArgs e)
    {
        e.Result = 100;
    }

    [TestMethod]
    public void TestInvalidExpressions()
    {
        this.ProcessScriptTests("InvalidExpressions.txt", DoTestInvalidExpressions);
    }

    [TestMethod]
    public void TestValidCasts()
    {
        MyCurrentContext = MyValidCastsContext;
        this.ProcessScriptTests("ValidCasts.txt", DoTestValidExpressions);
    }

    [TestMethod]
    public void TestCheckedExpressions()
    {
        this.ProcessScriptTests("CheckedTests.txt", DoTestCheckedExpressions);
    }

    private void DoTestValidExpressions(string[] arr)
    {
        var typeName = string.Concat("System.", arr[0]);
        var expressionType = Type.GetType(typeName, true, true);

        var context = MyCurrentContext;
        context.Options.ResultType = expressionType;
        context.Options.EmitToAssembly = true;

        var e = this.CreateDynamicExpression(arr[1], context);
        this.DoTest(e, arr[2], expressionType, Infrastructure.ExpressionTests.TestCulture);
    }

    private void DoTestInvalidExpressions(string[] arr)
    {
        var expressionType = Type.GetType(arr[0], true, true);
        var reason = (CompileExceptionReason)Enum.Parse(typeof(CompileExceptionReason), arr[2], true);

        var context = MyGenericContext;
        var options = context.Options;
        options.ResultType = expressionType;
        context.Imports.AddType(typeof(Math));
        options.OwnerMemberAccess = System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic;

        this.AssertCompileException(arr[1], context);
    }

    private void DoTestCheckedExpressions(string[] arr)
    {
        var expression = arr[0];
        var @checked = bool.Parse(arr[1]);
        var shouldOverflow = bool.Parse(arr[2]);

        var context = new ExpressionContext(MyValidExpressionsOwner);
        var options = context.Options;
        context.Imports.AddType(typeof(Math));
        context.Imports.ImportBuiltinTypes();
        options.Checked = @checked;

        try
        {
            var e = this.CreateDynamicExpression(expression, context);
            e.Evaluate();
            Assert.IsFalse(shouldOverflow, $"{expression} should overflow");
        }
        catch (OverflowException ex)
        {
            Assert.IsTrue(shouldOverflow, $"{expression} should not overflow");
        }
    }
}

}