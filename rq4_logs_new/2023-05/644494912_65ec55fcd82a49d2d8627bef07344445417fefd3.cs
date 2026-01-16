using System;
using nanoFramework.TestFramework;

namespace MakoIoT.Device.Utilities.Invoker.Test
{
    [TestClass]
    public class InvokerTest
    {
        [TestMethod]
        public void Retry_should_exit_on_successful_attempt()
        {
            int at = 0;
            Invoker.Retry(() =>
            {
                at++;
            }, 3);

            Assert.AreEqual(1, at);
        }

        [TestMethod]
        public void Retry_should_invoke_ExceptionDelegate_on_exception()
        {
            int at = 0;
            var e = new Exception("Test exception");
            Exception passedException = null;
            Invoker.Retry(() =>
            {
                if (at++ == 0)
                    throw e;
            }, 3, (exception, attempt) =>
            {
                passedException = exception;
                return true;
            });

            Assert.AreSame(e, passedException);
        }

        [TestMethod]
        public void Retry_given_ExceptionDelegate_returning_True_should_continue_after_exception()
        {
            int at = 0;
            Invoker.Retry(() =>
            {
                if (at++ < 2)
                    throw new Exception();
            }, 3, (exception, attempt) => true);

            Assert.AreEqual(3, at);
        }

        [TestMethod]
        public void Retry_given_throwIfFails_True_should_throw_last_exception()
        {
            int at = 0;
            Assert.ThrowsException(typeof(TestException), () =>
            {
                Invoker.Retry(() =>
                {
                    at++;
                    throw new TestException();
                }, 3, null);
            });

            Assert.AreEqual(3, at);
        }

        [TestMethod]
        public void Retry_given_ExceptionDelegate_returning_False_and_throwIfFails_True_should_throw_last_exception()
        {
            int at = 0;
            Assert.ThrowsException(typeof(TestException), () =>
            {
                Invoker.Retry(() =>
                {
                    at++;
                    throw new TestException();
                }, 3, (exception, attempt) => false);
            });
        }

        class TestException : Exception
        {
            
        }
    }
}