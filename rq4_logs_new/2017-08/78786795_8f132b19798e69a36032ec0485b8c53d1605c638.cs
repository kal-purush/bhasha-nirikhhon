using FDM90.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace FDM90.Handlers
{
    public interface ISchedulerHandler
    {
        void PostNow(ScheduledPost newPost);
        void CreateScheduledPost(ScheduledPost newPost);
        void UpdateScheduledPost(ScheduledPost updatedPost);
        IEnumerable<ScheduledPost> GetSchedulerPostsForUser(Guid userId);
        IEnumerable<ScheduledPost> GetSchedulerPostsForTime(DateTime currentTime);
        void DeleteScheduledPost(Guid postId);
        void DeletePostImage(Guid postId, string imagePath);
    }
}