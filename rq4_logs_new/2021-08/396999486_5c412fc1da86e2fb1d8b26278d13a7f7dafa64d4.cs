using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Text;
using todojuan.Functions.Functions;
using todojuan.Tests.Helpers;
using Xunit;

namespace todojuan.Tests.Tests
{
     public class ScheduledFunctionTest
    {

       

        [Fact]
        public void ScheduledFuncion_should_Log_Message()
        {
            //Arrange
            MockCloudTableTodo mockTodos = new MockCloudTableTodo(new Uri("http://127.0.0.1:10002/devstoreaccount1/reports"));
            ListLogger logger = (ListLogger)TestFactory.CreateLogger(LoggerTypes.List);
            //Act
            ScheduledFunction.Run(null, mockTodos, logger);
            string message = logger.Logs[0];

            //Asert

            Assert.Contains("Deleting complete todo", message);


        }
    }
}