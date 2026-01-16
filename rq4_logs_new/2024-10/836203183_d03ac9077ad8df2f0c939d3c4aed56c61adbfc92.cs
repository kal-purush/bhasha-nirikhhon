using FluentAssertions;
using System.Net;
using System.Net.Http.Json;
using WebApiDapperNativeAOT.Models.Responses;
using WebApiDapperNativeAOT.Routes;
using WebApiDapperNativeAOT.Testing.SeedWork;

namespace WebApiDapperNativeAOT.Testing.FuncionalTesting.API.Todo;

[Collection(nameof(TestServerFixtureCollection))]
public class Get(TestServerFixture fixture)
{
    private readonly TestServerFixture Fixture = fixture;

    [Fact]
    [Trait("Category", "Functional")]
    public async Task GetTodoById_Should_ReturnTodo_Ok()
    {
        // Arrange
        var defaultTodo = await Fixture.Extension.AddDefaultTodo();

        // Act
        var response = await Fixture.HttpClient.GetAsync(ApiRoutes.Todo.GetById(defaultTodo.Id));

        // Assert
        response.StatusCode.Should().Be(HttpStatusCode.OK);
        var responseModel = await response.Content.ReadFromJsonAsync<TodoResponse>();
        responseModel.Should().NotBeNull();
        responseModel!.Id.Should().Be(defaultTodo.Id);
        responseModel.Title.Should().Be(defaultTodo.Title);
        responseModel.Description.Should().Be(defaultTodo.Description);
        responseModel.CreatedBy.Should().Be(defaultTodo.CreatedBy);
        responseModel.AssignedTo.Should().Be(defaultTodo.AssignedTo);
        responseModel.TargetDate.Should().Be(defaultTodo.TargetDate);
        responseModel.IsComplete.Should().Be(defaultTodo.IsComplete);
    }
}