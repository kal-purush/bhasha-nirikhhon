using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using PetFamily.Core.Abstractions;
using PetFamily.Discussions.Application.Commands.AddMessage;
using PetFamily.IntegrationTests.Discussions.Heritage;
using PetFamily.IntegrationTests.General;

namespace PetFamily.IntegrationTests.Discussions.HandlerTests;

public class AddMessageHandlerTests : DiscussionsTestsBase
{
    private readonly ICommandHandler<Guid, AddMessageCommand> _sut;

    public AddMessageHandlerTests(DiscussionsWebFactory factory) : base(factory)
    {
        _sut = Scope.ServiceProvider.GetRequiredService<ICommandHandler<Guid, AddMessageCommand>>();
    }

    [Fact]
    public async Task AddMessage_success_should_add_message_to_discussion_without_messages()
    {
        // arrange
        var USER_COUNT = 2;
        var MESSAGE_TEXT = "Test text";
        var discussion = await DataGenerator.SeedDiscussion(WriteDbContext, USER_COUNT);

        var command = new AddMessageCommand(discussion.RelationId, discussion.UserIds[0], MESSAGE_TEXT);

        // act
        var result = await _sut.HandleAsync(command, CancellationToken.None);

        // assert
        result.IsSuccess.Should().BeTrue();
        var discussionResult = await WriteDbContext.Discussions.FirstOrDefaultAsync(d => d.Id == discussion.Id);
        discussionResult.Should().NotBeNull();
        discussionResult.Messages.Count().Should().Be(1);
        discussionResult.Messages.First().Text.Value.Should().Be(MESSAGE_TEXT);
    }

    [Fact]
    public async Task AddMessage_success_should_add_message_to_discussion_that_already_have_messages()
    {
        // arrange
        var USER_COUNT = 2;
        var MESSAGE_TEXT1 = "Test text2";
        var MESSAGE_TEXT2 = "Test text2";
        var discussion = await DataGenerator.SeedDiscussion(WriteDbContext, USER_COUNT);
        var message1 = DataGenerator.CreateMessage(discussion.UserIds[0], MESSAGE_TEXT1);
        discussion.Messages.Add(message1);
        await WriteDbContext.SaveChangesAsync();

        var command = new AddMessageCommand(discussion.RelationId, discussion.UserIds[1], MESSAGE_TEXT2);

        // act
        var result = await _sut.HandleAsync(command, CancellationToken.None);

        // assert
        result.IsSuccess.Should().BeTrue();
        var discussionResult = await WriteDbContext.Discussions.FirstOrDefaultAsync(d => d.Id == discussion.Id);
        discussionResult.Should().NotBeNull();
        discussionResult.Messages.Count().Should().Be(2);
        discussionResult.Messages.Should().Contain(m => m.Text.Value == MESSAGE_TEXT1);
        discussionResult.Messages.Should().Contain(m => m.Text.Value == MESSAGE_TEXT2);
    }

    [Fact]
    public async Task AddMessage_failure_should_not_add_message_because_sender_is_not_a_member_of_discssuion()
    {
        // arrange
        var USER_COUNT = 2;
        var MESSAGE_TEXT = "Test text";
        var discussion = await DataGenerator.SeedDiscussion(WriteDbContext, USER_COUNT);

        var command = new AddMessageCommand(discussion.RelationId, Guid.NewGuid(), MESSAGE_TEXT);

        // act
        var result = await _sut.HandleAsync(command, CancellationToken.None);

        // assert
        result.IsFailure.Should().BeTrue();
        var discussionResult = await WriteDbContext.Discussions.FirstOrDefaultAsync(d => d.Id == discussion.Id);
        discussionResult.Should().NotBeNull();
        discussionResult.Messages.Count().Should().Be(0);
    }
}