using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Primitives;
using Moq;
using Rinkudesu.Gateways.Webui.Middleware;

namespace Rinkudesu.Gateways.Webui.Tests;

public class ReturnUrlValidationMiddlewareTests
{
    private IFormCollection? lastSetForm;
    private QueryString? lastSetQuery;

    private bool nextCalled;
    private readonly ReturnUrlValidationMiddleware _middleware;

    public ReturnUrlValidationMiddlewareTests()
    {
        _middleware = new(_ =>
        {
            nextCalled = true;
            return Task.CompletedTask;
        });
    }

    [Fact]
    public async Task InvokeAsync_QueryNoReturnUrlInRequest_ReturnsUnchanged()
    {
        var context = GetMockContext(new Dictionary<string, StringValues>
        {
            { "aaa", "bbb" },
            { "ccc", "ddd" }
        }, null);

        await _middleware.InvokeAsync(context);

        Assert.True(nextCalled);
        Assert.Null(lastSetForm);
        Assert.False(lastSetQuery.HasValue);
    }

    [Theory]
    [InlineData("this%is/a test")]
    [InlineData("/test")]
    [InlineData("/test/another")]
    [InlineData("/test/another/WhyDoIDoThis")]
    public async Task InvokeAsync_ReturnUrlNotParsableOrRelative_RemainsUnchanged(string value)
    {
        var dictionary = new Dictionary<string, StringValues>
        {
            { "argument", "and-a-value" },
            { "returnUrl", value },
            { "another", "not_to_remove" }
        };
        var context = GetMockContext(dictionary, dictionary);

        await _middleware.InvokeAsync(context);

        Assert.True(nextCalled);
        Assert.NotNull(lastSetForm);
        Assert.True(lastSetQuery.HasValue);
        Assert.Equal(value, lastSetForm!["returnUrl"]);
        Assert.Contains($"&returnUrl={value}&", lastSetQuery!.Value.Value!);
    }

    [Theory]
    [InlineData("this%is/a test")]
    [InlineData("/test")]
    [InlineData("/test/another")]
    [InlineData("/test/another/WhyDoIDoThis")]
    public async Task InvokeAsync_ReturnUrlAsFirstNotParsableOrRelative_RemainsUnchanged(string value)
    {
        var dictionary = new Dictionary<string, StringValues>
        {
            { "returnUrl", value },
            { "argument", "and-a-value" },
            { "another", "not_to_remove" }
        };
        var context = GetMockContext(dictionary, dictionary);

        await _middleware.InvokeAsync(context);

        Assert.True(nextCalled);
        Assert.NotNull(lastSetForm);
        Assert.True(lastSetQuery.HasValue);
        Assert.Equal(value, lastSetForm!["returnUrl"]);
        Assert.Contains($"?returnUrl={value}&", lastSetQuery!.Value.Value!);
    }

    [Theory]
    [InlineData("://sd\r\nfjkalskjdhfklasdf//kasjhdfklajhsdlfkjashdlfkjshf")]
    [InlineData("/test")]
    [InlineData("/test/another")]
    [InlineData("/test/another/WhyDoIDoThis")]
    public async Task InvokeAsync_ReturnUrlAsLastNotParsableOrRelative_RemainsUnchanged(string value)
    {
        var dictionary = new Dictionary<string, StringValues>
        {
            { "argument", "and-a-value" },
            { "another", "not_to_remove" },
            { "returnUrl", value }
        };
        var context = GetMockContext(dictionary, dictionary);

        await _middleware.InvokeAsync(context);

        Assert.True(nextCalled);
        Assert.NotNull(lastSetForm);
        Assert.True(lastSetQuery.HasValue);
        Assert.Equal(value, lastSetForm!["returnUrl"]);
        Assert.Contains($"&returnUrl={value}", lastSetQuery!.Value.Value!);
    }

    [Theory]
    [InlineData("/test")]
    [InlineData("/test/another")]
    [InlineData("/test/another/WhyDoIDoThis")]
    public async Task InvokeAsync_ReturnUrlAbsolute_PathOnly(string value)
    {
        var dictionary = new Dictionary<string, StringValues>
        {
            { "returnUrl", $"https://localhost.localdomain{value}" },
            { "argument", "and-a-value" },
            { "another", "not_to_remove" }
        };
        var context = GetMockContext(dictionary, dictionary);

        await _middleware.InvokeAsync(context);

        Assert.True(nextCalled);
        Assert.NotNull(lastSetForm);
        Assert.True(lastSetQuery.HasValue);
        Assert.Equal(value, lastSetForm!["returnUrl"]);
        Assert.Contains($"?returnUrl={value}&", lastSetQuery!.Value.Value!);
    }

    [Fact]
    public async Task InvokeAsync_FormNoReturnUrlInRequest_ReturnsUnchanged()
    {
        var context = GetMockContext(null, new Dictionary<string, StringValues>
        {
            { "aaa", "bbb" },
            { "ccc", "ddd" }
        });

        await _middleware.InvokeAsync(context);

        Assert.True(nextCalled);
        Assert.Null(lastSetForm);
        Assert.False(lastSetQuery.HasValue);
    }

    private HttpContext GetMockContext(Dictionary<string, StringValues>? query, Dictionary<string, StringValues>? form)
    {
        var mockContext = new Mock<HttpContext>();
        var mockRequest = new Mock<HttpRequest>();

        mockRequest.SetupSet(r => r.Form = It.IsAny<IFormCollection>()).Callback((IFormCollection f) => lastSetForm = f);
        mockRequest.SetupSet(r => r.QueryString = It.IsAny<QueryString>()).Callback((QueryString q) => lastSetQuery = q);

        if (query is not null)
        {
            var queryCollection = new QueryCollection(query);
            var queryString = $"?{string.Join('&', queryCollection.Select(q => $"{q.Key}={q.Value}"))}";
            mockRequest.SetupGet(r => r.QueryString).Returns(new QueryString(queryString));
            mockRequest.SetupGet(r => r.Query).Returns(queryCollection);
        }
        else
        {
            mockRequest.SetupGet(r => r.Query).Returns(QueryCollection.Empty);
        }
        if (form is not null)
        {
            var formCollection = new FormCollection(form);
            mockRequest.SetupGet(r => r.Form).Returns(formCollection);
            mockRequest.SetupGet(r => r.HasFormContentType).Returns(true);
        }

        mockContext.SetupGet(c => c.Request).Returns(mockRequest.Object);
        return mockContext.Object;
    }
}