using FluentAssertions;
using Microsoft.AspNetCore.Http;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using Moq;
using MyRestaurant.Api.Middleware;
using MyRestaurant.Business.Errors;
using System;
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Reflection;
using System.Threading.Tasks;
using Xunit;

namespace MyRestaurant.Api.Tests.Middleware
{
    public class ExceptionMiddlewareTests
    {
        [Fact]
        public async Task ExceptionMiddleware_Capture_Rest_Exception()
        {
            // Arrange
            var defaultContext = new DefaultHttpContext();
            defaultContext.Response.Body = new MemoryStream();
            var loggerMock = new Mock<ILogger<ExceptionMiddleware>>();

            // Act
            var middlewareInstance = new ExceptionMiddleware(next: (innerHttpContext) =>
            {
                throw new RestException(HttpStatusCode.NotFound, "Unit of measure not found.");
            }, logger: loggerMock.Object);

            await middlewareInstance.InvokeAsync(defaultContext);

            // Assert
            defaultContext.Response.Body.Seek(0, SeekOrigin.Begin);
            var body = new StreamReader(defaultContext.Response.Body).ReadToEnd();
            body.Should().Contain("404");
            body.Should().Contain("Unit of measure not found.");
            body.Should().Contain("errorCode");
            body.Should().Contain("errorType");
            body.Should().Contain("errorMessage");
            body.Should().Contain("errorDate");
        }

        [Fact]
        public async Task ExceptionMiddleware_Capture_Any_Exception()
        {
            // Arrange
            var defaultContext = new DefaultHttpContext();
            defaultContext.Response.Body = new MemoryStream();
            var loggerMock = new Mock<ILogger<ExceptionMiddleware>>();

            // Act
            var middlewareInstance = new ExceptionMiddleware(next: (innerHttpContext) =>
            {
                throw new Exception("Server Error", new Exception("Inner exception error message"));
            }, logger: loggerMock.Object);

            await middlewareInstance.InvokeAsync(defaultContext);

            // Assert
            defaultContext.Response.Body.Seek(0, SeekOrigin.Begin);
            var body = new StreamReader(defaultContext.Response.Body).ReadToEnd();
            body.Should().Contain("500");
            body.Should().Contain("Inner exception error message");
            body.Should().Contain("errorCode");
            body.Should().Contain("errorType");
            body.Should().Contain("errorMessage");
            body.Should().Contain("errorDate");
        }

        [Fact]
        public async Task ExceptionMiddleware_Capture_ForeignKey_Reference_Sql_Exception()
        {
            // Arrange
            var defaultContext = new DefaultHttpContext();
            defaultContext.Response.Body = new MemoryStream();
            var loggerMock = new Mock<ILogger<ExceptionMiddleware>>();

            // Act
            var middlewareInstance = new ExceptionMiddleware(next: (innerHttpContext) =>
            {
                throw MakeSqlException(547);
            }, logger: loggerMock.Object);

            await middlewareInstance.InvokeAsync(defaultContext);

            // Assert
            defaultContext.Response.Body.Seek(0, SeekOrigin.Begin);
            var body = new StreamReader(defaultContext.Response.Body).ReadToEnd();
            body.Should().Contain("409");
            body.Should().Contain("This item cannot be deleted.");
            body.Should().Contain("errorCode");
            body.Should().Contain("errorType");
            body.Should().Contain("errorMessage");
            body.Should().Contain("errorDate");
        }

        private SqlException MakeSqlException(int sqlNumber)
        {
            //create new exception
            var newException = new Exception();

            //construct the SQL Error Collection
            SqlErrorCollection collection = Construct<SqlErrorCollection>();
            SqlError error = Construct<SqlError>(sqlNumber, (byte)2, (byte)3, "server name", "error message", "proc", 100, newException);

            //Using reflection inject the error into the SqlErrorCollection
            typeof(SqlErrorCollection).GetMethod("Add", BindingFlags.NonPublic | BindingFlags.Instance)
                .Invoke(collection, new object[] { error });

            //Using reflection create the SQL exception
            var e = typeof(SqlException).GetMethod("CreateException", BindingFlags.NonPublic | BindingFlags.Static, null,
                CallingConventions.ExplicitThis, new[] { typeof(SqlErrorCollection), typeof(string) },
                new ParameterModifier[] { }).Invoke(null, new object[] { collection, "11.0.0" }) as SqlException;

            return e;
        }

        //constructs a class based off of the type and parameters
        private T Construct<T>(params object[] p)
        {
            //declare a new var for type
            var t = new Type[p.Length];

            //for each parameter get the type
            for (var i = 0; i < p.Length; i++)
            {
                t[i] = p[i].GetType();
            }

            //get constructor info 
            var constructorInfo = typeof(T).GetConstructor(BindingFlags.NonPublic | BindingFlags.Instance, null, t, null);

            if (constructorInfo == null)
            {
                throw new InvalidOperationException(string.Format("Cannot find a matching private or static constructor for type {0} with the constructor parameters ({1})",
                    typeof(T).Name, string.Join(", ", t.AsEnumerable())));
            }

            //invoke and return constructor
            return (T)constructorInfo.Invoke(p);
        }
    }
}