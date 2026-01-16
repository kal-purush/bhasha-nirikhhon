using CSharpFunctionalExtensions;
using FluentAssertions;
using PetFamily.Discussions.Domain.Entities;
using PetFamily.Discussions.Domain.ValueObjects;
using PetFamily.SharedKernel.CustomErrors;
using PetFamily.SharedKernel.ValueObjects.Ids;
using PetFamily.Volunteers.Domain.ValueObjects.PetVO;

namespace PetFamily.UnitTests;

public class DiscussionsTests
{
    [Fact]
    public void Create_success_should_create_discussion()
    {
        // Arrange
        const int USER_COUNT = 2;

        // Act
        var result = CreateDiscussion(USER_COUNT);

        // Assert
        result.IsSuccess.Should().BeTrue();
        result.Value.UserIds.Count.Should().Be(USER_COUNT);
    }
    
    [Fact]
    public void Create_failure_should_return_error_because_too_few_users_to_create_discussion()
    {
        // Arrange
        const int USER_COUNT = 1;

        // Act
        var result = CreateDiscussion(USER_COUNT);

        // Assert
        result.IsFailure.Should().BeTrue();
    }
    
    [Fact]
    public void AddMessage_success_should_add_message_to_discussion_with_empty_messages()
    {
        // Arrange
        const int USER_COUNT = 2;
        const string MESSAGE = "First Message";
        var discussion = CreateDiscussion(USER_COUNT).Value;
        var message = CreateMessage(discussion.UserIds[0], MESSAGE);
        
        // Act
        var result = discussion.AddMessage(message);

        // Assert
        result.IsSuccess.Should().BeTrue();
        discussion.Messages.Should().NotBeNull();
        
        var addedMessage = discussion.Messages.FirstOrDefault();
        addedMessage.UserId.Should().Be(message.UserId);
        addedMessage.Text.Value.Should().Be(MESSAGE);
    }
    
    [Fact]
    public void AddMessage_success_should_add_message_to_discussion_that_already_have_messages()
    {
        // Arrange
        const int USER_COUNT = 2;
        const string MESSAGE1 = "First Message";
        const string MESSAGE2 = "Second Message";
        var discussion = CreateDiscussion(USER_COUNT).Value;
        var message1 = CreateMessage(discussion.UserIds[0], MESSAGE1);
        var message2 = CreateMessage(discussion.UserIds[1], MESSAGE2);
        discussion.AddMessage(message1);
        
        // Act
        var result = discussion.AddMessage(message2);

        // Assert
        result.IsSuccess.Should().BeTrue();
        discussion.Messages.Should().NotBeNull();
        discussion.Messages.Count.Should().Be(2);
        
        var addedMessage = discussion.Messages.FirstOrDefault(m => m.Id == message2.Id);
        addedMessage.UserId.Should().Be(message2.UserId);
        addedMessage.Text.Value.Should().Be(MESSAGE2);
    }
    
    [Fact]
    public void AddMessage_failure_should_return_error_because_author_of_message_doesnt_belong_to_discussion()
    {
        // Arrange
        const int USER_COUNT = 2;
        const string MESSAGE = "First Message";
        var discussion = CreateDiscussion(USER_COUNT).Value;
        var message = CreateMessage(UserId.NewUserId(), MESSAGE);
        
        // Act
        var result = discussion.AddMessage(message);

        // Assert
        result.IsFailure.Should().BeTrue();
        discussion.Messages.Should().BeEmpty();
    }
    
    [Fact]
    public void RemoveMessage_success_should_remove_message_from_discussion()
    {
        // Arrange
        const int USER_COUNT = 2;
        const string MESSAGE1 = "First Message";
        const string MESSAGE2 = "Second Message";
        var discussion = CreateDiscussion(USER_COUNT).Value;
        var message1 = CreateMessage(discussion.UserIds[0], MESSAGE1);
        var message2 = CreateMessage(discussion.UserIds[1], MESSAGE2);
        discussion.AddMessage(message1);
        discussion.AddMessage(message2);
        
        // Act
        var result = discussion.RemoveMessage(discussion.UserIds[0], message1.Id);

        // Assert
        result.IsSuccess.Should().BeTrue();
        discussion.Messages.Count.Should().Be(1);
        
        var removedMessage = discussion.Messages.FirstOrDefault(m => m.Id == message1.Id);
        removedMessage.Should().BeNull();
    }
    
    [Fact]
    public void RemoveMessage_failure_should_return_error_because_user_that_removes_doesnt_own_message()
    {
        // Arrange
        const int USER_COUNT = 2;
        const string MESSAGE1 = "First Message";
        const string MESSAGE2 = "Second Message";
        var discussion = CreateDiscussion(USER_COUNT).Value;
        var message1 = CreateMessage(discussion.UserIds[0], MESSAGE1);
        var message2 = CreateMessage(discussion.UserIds[1], MESSAGE2);
        discussion.AddMessage(message1);
        discussion.AddMessage(message2);
        
        // Act
        var result = discussion.RemoveMessage(discussion.UserIds[1], message1.Id);

        // Assert
        result.IsFailure.Should().BeTrue();
        discussion.Messages.Count.Should().Be(2);
    }
    
    [Fact]
    public void EditMessage_success_should_edit_the_message_in_discussion()
    {
        // Arrange
        const int USER_COUNT = 2;
        const string MESSAGE1 = "First Message";
        const string MESSAGE2 = "Second Message";
        const string EDITED_MESSAGE = "Edit Message";
        var discussion = CreateDiscussion(USER_COUNT).Value;
        var message1 = CreateMessage(discussion.UserIds[0], MESSAGE1);
        var message2 = CreateMessage(discussion.UserIds[1], MESSAGE2);
        discussion.AddMessage(message1);
        discussion.AddMessage(message2);
        
        // Act
        var result = discussion.EditMessage(discussion.UserIds[0], message1.Id, MessageText.Create(EDITED_MESSAGE).Value);

        // Assert
        result.IsSuccess.Should().BeTrue();
        discussion.Messages.Count.Should().Be(2);
        
        var editedMessage = discussion.Messages.FirstOrDefault(m => m.Id == message1.Id);
        editedMessage.IsEdited.Value.Should().BeTrue();
        editedMessage.Text.Value.Should().Be(EDITED_MESSAGE);
    }
    
    [Fact]
    public void EditMessage_failure_should_return_error_because_user_that_edits_doesnt_own_message()
    {
        // Arrange
        const int USER_COUNT = 2;
        const string MESSAGE1 = "First Message";
        const string MESSAGE2 = "Second Message";
        const string EDITED_MESSAGE = "Edit Message";
        var discussion = CreateDiscussion(USER_COUNT).Value;
        var message1 = CreateMessage(discussion.UserIds[0], MESSAGE1);
        var message2 = CreateMessage(discussion.UserIds[1], MESSAGE2);
        discussion.AddMessage(message1);
        discussion.AddMessage(message2);
        
        // Act
        var result = discussion.EditMessage(discussion.UserIds[1], message1.Id, MessageText.Create(EDITED_MESSAGE).Value);

        // Assert
        result.IsFailure.Should().BeTrue();
        
        var editedMessage = discussion.Messages.FirstOrDefault(m => m.Id == message1.Id);
        editedMessage.IsEdited.Value.Should().BeFalse();
        editedMessage.Text.Value.Should().Be(MESSAGE1);
    }
    
    [Fact]
    public void Close_Success_should_set_status_of_discussion_to_closed()
    {
        // Arrange
        const int USER_COUNT = 2;
        const string MESSAGE1 = "First Message";
        var discussion = CreateDiscussion(USER_COUNT).Value;
        var message1 = CreateMessage(discussion.UserIds[0], MESSAGE1);
        discussion.AddMessage(message1);
        
        // Act
        discussion.Close();

        // Assert
        discussion.Status.Value.Should().Be(DiscussionStatusEnum.Closed);
    }
    
    public static Result<Discussion, Error> CreateDiscussion(int userCount)
    {
        var users = new List<UserId>();
        for (var i = 0; i < userCount; i++)
            users.Add(UserId.NewUserId());
        
        var relationId = RelationId.NewRelationId();

        var result = Discussion.Create(relationId, users);

        return result;
    }

    public static Message CreateMessage(UserId userId, string text)
    {
        var messageText = MessageText.Create(text).Value;
        var message = new Message(messageText, userId);
        
        return message;
    }
}