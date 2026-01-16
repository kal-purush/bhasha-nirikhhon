using AskHand.Domain.Users;

namespace AskHand.Domain.Friendships;

public sealed class Friendship : Entity<FriendshipId>
{
    public UserId UserIdOne { get; }

    public UserId UserIdTwo { get; }

    public DateTime StartFriendshipDate { get; }

    public DateTime? EndFriendshipDate { get; }

    private Friendship(
        FriendshipId id,
        UserId userIdOne,
        UserId userIdTwo, 
        DateTime startFriendshipDate, 
        DateTime endFriendshipDate) 
        : base(id)
    {
        UserIdOne = userIdOne;
        UserIdTwo = userIdTwo;
        StartFriendshipDate = startFriendshipDate;
        EndFriendshipDate = endFriendshipDate;
    }

    public static Friendship Create(
        UserId userIdOne, 
        UserId userIdTwo, 
        DateTime startFriendshipDate, 
        DateTime endFriendshipDate)
    {
        return new Friendship(
            FriendshipId.CreateUnique(), 
            userIdOne, 
            userIdTwo, 
            startFriendshipDate, 
            endFriendshipDate);
    }
}