using Application.Completions;
using BuildingBlocks.Application.Ports;
using Moq;

public class TextCompletionQueryHandlerTests
{
    [Fact]
    public async Task Handle_ShouldReturnExpectedResult_WhenPromptIsProvided()
    {
        // Arrange
        var mockChatCompletionProvider = new Mock<IChatCompletionProvider>();
        var expectedResponse = "Completed response";
        var prompt = "Test prompt";

        mockChatCompletionProvider
            .Setup(provider => provider.CompleteChatAsync(prompt))
            .ReturnsAsync(expectedResponse);

        var handler = new TextCompletionQueryHandler(mockChatCompletionProvider.Object);
        var query = new TextCompletionQuery(prompt);
        var cancellationToken = CancellationToken.None;

        // Act
        var result = await handler.Handle(query, cancellationToken);

        // Assert
        Assert.Equal(expectedResponse, result);
        mockChatCompletionProvider.Verify(provider => provider.CompleteChatAsync(prompt), Times.Once);
    }

    [Fact]
    public async Task Handle_ShouldThrowException_WhenProviderThrowsException()
    {
        // Arrange
        var mockChatCompletionProvider = new Mock<IChatCompletionProvider>();
        var prompt = "Test prompt";

        mockChatCompletionProvider
            .Setup(provider => provider.CompleteChatAsync(prompt))
            .ThrowsAsync(new System.Exception("Provider error"));

        var handler = new TextCompletionQueryHandler(mockChatCompletionProvider.Object);
        var query = new TextCompletionQuery(prompt);
        var cancellationToken = CancellationToken.None;

        // Act & Assert
        await Assert.ThrowsAsync<System.Exception>(() => handler.Handle(query, cancellationToken));
        mockChatCompletionProvider.Verify(provider => provider.CompleteChatAsync(prompt), Times.Once);
    }
}