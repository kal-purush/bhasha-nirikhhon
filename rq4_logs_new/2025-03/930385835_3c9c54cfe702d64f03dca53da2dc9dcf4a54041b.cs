using CSharpFunctionalExtensions;
using PetFamily.Discussions.Domain.ValueObjects;
using PetFamily.SharedKernel.CustomErrors;
using PetFamily.SharedKernel.ValueObjects.Ids;

namespace PetFamily.Discussions.Domain.Entities;

public class Discussion
{
    public DiscussionId Id { get; private set; }
    public RelationId RelationId { get; private set; }
    public List<UserId> UserIds { get; private set; }
    public List<Message> Messages { get; private set; } = [];
    public DiscussionStatus Status { get; private set; }
    
    private Discussion() { } // ef core
    
    private Discussion(RelationId relationId, List<UserId> userIds)
    {
        Id = DiscussionId.NewDiscussionId();
        RelationId = relationId;
        UserIds = userIds;
        Status = DiscussionStatus.Create(DiscussionStatusEnum.Opened).Value;
    }

    public static Result<Discussion, Error> Create(RelationId relationId, List<UserId> userIds)
    {
        if (userIds.Count < 2)
            return Errors.General
                .ValueIsInvalid("userIds", "discussion should have at least 2 users");
        
        return new Discussion(relationId, userIds);
    }

    public UnitResult<Error> AddMessage(Message message)
    {
        if (Status.Value == DiscussionStatusEnum.Closed)
            return Errors.General.Conflict("cannot add message to closed discussion");

        if (UserIds.All(u => u != message.UserId))
        {
            return Errors.General.Conflict("user cannot add messages to discussions he doesn't belong to");
        }
        
        Messages.Add(message);
        return UnitResult.Success<Error>();
    }

    public UnitResult<Error> RemoveMessage(UserId userId, MessageId messageId)
    {
        if (Status.Value == DiscussionStatusEnum.Closed)
            return Errors.General.Conflict("cannot remove message from closed discussion");
        
        var message = Messages.FirstOrDefault(m => m.Id == messageId);
        if (message == null)
            return Errors.General.ValueNotFound("message");

        if (message.UserId != userId)
            return Errors.General.Conflict("user cannot remove messages that were not created by him");
        
        Messages.Remove(message);
        return UnitResult.Success<Error>();
    }

    public UnitResult<Error> EditMessage(UserId userId, MessageId editMessageId, MessageText newMessageText)
    {
        if (Status.Value == DiscussionStatusEnum.Closed)
            return Errors.General.Conflict("cannot remove message from closed discussion");
        
        var message = Messages.FirstOrDefault(m => m.Id == editMessageId);
        if (message == null)
            return Errors.General.Conflict("message not found");
        
        if (message.UserId != userId)
            return Errors.General.Conflict("user cannot edit messages that were not created by him");
        
        message.Edit(newMessageText);
        return UnitResult.Success<Error>();
    }

    public void Close()
    {
        Status = DiscussionStatus.Create(DiscussionStatusEnum.Closed).Value;
    }
}