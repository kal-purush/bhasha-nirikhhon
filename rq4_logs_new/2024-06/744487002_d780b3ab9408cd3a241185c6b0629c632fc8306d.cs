namespace Server.Model.Responses.User;

public record FriendHubFriend(string RequestId, string SenderName, string SenderId, string SentTime, string ReceiverName, string ReceiverId);