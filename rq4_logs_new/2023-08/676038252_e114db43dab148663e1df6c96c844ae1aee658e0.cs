using System.Collections.Generic;
using FacebookWrapper.ObjectModel;

namespace BasicFacebookFeatures.Features
{
    public static class FriendsAnalyticsFeature
    {
        public static int GetNumberOfEngagementsFromFriend(User i_User, User i_Friend, eEngagmentType i_EngagmentType)
        {
            int numberOfEngagments = 0;

            foreach(Post post in i_User.Posts)
            {
                if(i_EngagmentType == eEngagmentType.Likes || i_EngagmentType == eEngagmentType.All)
                {
                    if(post.LikedBy.Contains(i_Friend))
                    {
                        numberOfEngagments++;
                    }
                }
                
                if(i_EngagmentType == eEngagmentType.Comments || i_EngagmentType == eEngagmentType.All)
                {
                    foreach(Comment comment in post.Comments)
                    {
                        if(comment.From.Id == i_Friend.Id)
                        {
                            numberOfEngagments++;
                        }
                    }
                }
            }

            return numberOfEngagments;
        }
    }
}