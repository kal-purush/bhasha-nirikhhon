using FacebookWrapper.ObjectModel;

namespace BasicFacebookFeatures
{
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
}