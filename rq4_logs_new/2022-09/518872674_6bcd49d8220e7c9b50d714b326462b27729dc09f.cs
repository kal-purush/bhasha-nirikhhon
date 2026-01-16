using FacebookWrapper.ObjectModel;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace BasicFacebookFeatures
{
    public abstract class CreatePost
    {
        public abstract void Post(PostParameters i_PostDetails, User i_LoggedInUser);
        public static void CreateNewPost(PostParameters i_PostDetails, User i_LoggedInUser)
        {

            switch (i_PostDetails.PostType)
            {
                case ePostType.Text:
                    new CreateTextPost(i_PostDetails, i_LoggedInUser);
                    break;
                case ePostType.Image:
                    new CreateImagePost(i_PostDetails, i_LoggedInUser);
                    break;
                case ePostType.Link:
                    new CreateLinkPost(i_PostDetails, i_LoggedInUser);
                    break;
                default:
                    break;
            }
        }
    }

    public class CreateTextPost : CreatePost
    {
        public CreateTextPost(PostParameters i_PostDetails, User i_LoggedInUser)
        {
            Post(i_PostDetails, i_LoggedInUser);
        }

        public override void Post(PostParameters i_PostDetails, User i_LoggedInUser)
        {
            i_LoggedInUser.PostStatus(i_PostDetails.Text);
        }
    }

    public class CreateImagePost : CreatePost
    {
        public CreateImagePost(PostParameters i_PostDetails, User i_LoggedInUser)
        {
            Post(i_PostDetails, i_LoggedInUser);
        }

        public override void Post(PostParameters i_PostDetails, User i_LoggedInUser)
        {
            i_LoggedInUser.PostStatus(i_PostDetails.Text, null, i_PostDetails.ImgUrl);
        }
    }

    public class CreateLinkPost : CreatePost
    {
        public CreateLinkPost(PostParameters i_PostDetails, User i_LoggedInUser)
        {
            Post(i_PostDetails, i_LoggedInUser);
        }

        public override void Post(PostParameters i_PostDetails, User i_LoggedInUser)
        {
            i_LoggedInUser.PostStatus(i_PostDetails.Text, null, null, null, i_PostDetails.LinkUrl);
        }
    }
}