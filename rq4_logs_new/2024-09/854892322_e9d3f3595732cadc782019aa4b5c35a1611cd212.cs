using Rare.Models;

namespace Rare.Data
{
    public class SubscriptionData
    {
        public static List<Subscription> Subscriptions = new()
        {
            new() { Id = 1, AuthorId = 4, FollowerId = 1, CreatedOn = new DateTime(2024, 08, 10) },
            new() { Id = 2, AuthorId = 4, FollowerId = 3, CreatedOn = new DateTime(2024, 08, 12) },
            new() { Id = 3, AuthorId = 4, FollowerId = 5, CreatedOn = new DateTime(2024, 08, 15) },
            new() { Id = 4, AuthorId = 2, FollowerId = 1, CreatedOn = new DateTime(2024, 08, 20) },
            new() { Id = 5, AuthorId = 5, FollowerId = 3, CreatedOn = new DateTime(2024, 08, 10), EndedOn = new DateTime(2024, 09, 02) },
            new() { Id = 6, AuthorId = 5, FollowerId = 4, CreatedOn = new DateTime(2024, 08, 01) },
        };
    }
}