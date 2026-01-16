using FluentAssertions;
using Microsoft.EntityFrameworkCore;
using PetFamily.Discussions.Domain.Entities;
using PetFamily.Discussions.Domain.ValueObjects;
using PetFamily.IntegrationTests.Discussions.Heritage;
using PetFamily.IntegrationTests.General;
using PetFamily.SharedKernel.ValueObjects.Ids;

namespace PetFamily.IntegrationTests.Discussions.HandlerTests;

public class TestsOfModelBinding : DiscussionsTestsBase
{
    public TestsOfModelBinding(DiscussionsWebFactory factory) : base(factory)
    {
    }

    [Fact]
    public async Task Test_should_add_to_2_discussions_to_database()
    {
        // arrange
        const int FIRST_DISCUSSION_USER_COUNT = 3;
        const int SECOND_DISCUSSION_USER_COUNT = 2;
        
        // act
        var discussion1 = await DataGenerator.SeedDiscussion(WriteDbContext, FIRST_DISCUSSION_USER_COUNT);
        var discussion2 = await DataGenerator.SeedDiscussion(WriteDbContext, SECOND_DISCUSSION_USER_COUNT);

        // assert
        WriteDbContext.Discussions.Count().Should().Be(2);
    }
    
    [Fact]
    public async Task Test_should_add_messages_to_discussion()
    {
        // arrange
        const int USER_COUNT = 2;
        const string MESSAGE_1 = "Message 1";
        const string MESSAGE_2 = "Message 2";
        
        var baseDiscussion = await DataGenerator.SeedDiscussion(WriteDbContext, USER_COUNT);
        
        var userId1 = baseDiscussion.UserIds[0];
        var userId2 = baseDiscussion.UserIds[1];
        var messageText1 = MessageText.Create(MESSAGE_1).Value;
        var messageText2 = MessageText.Create(MESSAGE_2).Value;
        var message1 = new Message(messageText1, userId1);
        var message2 = new Message(messageText2, userId2);
        
        var discussionDbArrange = await WriteDbContext.Discussions.FirstOrDefaultAsync();
        
        var result1 = discussionDbArrange.AddMessage(message1);
        var result2 = discussionDbArrange.AddMessage(message2);
        
        // act
        await WriteDbContext.SaveChangesAsync();

        // assert
        var discussionAssert = await WriteDbContext.Discussions.FirstOrDefaultAsync();
        discussionAssert.Should().NotBeNull();
        discussionAssert.Messages.Count().Should().Be(2);
        discussionAssert.Messages[0].Text.Value.Should().Be(MESSAGE_1);
        discussionAssert.Messages[1].Text.Value.Should().Be(MESSAGE_2);
        discussionAssert.Messages[0].UserId.Should().Be(userId1);
        discussionAssert.Messages[1].UserId.Should().Be(userId2);
    }
}