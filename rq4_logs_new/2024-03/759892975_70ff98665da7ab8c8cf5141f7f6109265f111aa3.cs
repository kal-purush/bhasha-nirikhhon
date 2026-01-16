using YouCode.BE;
using YouCode.BL;

namespace YouCode.GUI.Services
{
    public class PostService
    {
        private readonly UserBL userBL = new UserBL();
        private readonly ProfileBL profileBL = new ProfileBL();
        private readonly PostBL postBL = new PostBL();
        private readonly ImageBL imageBL = new ImageBL();
        private readonly FavoriteBL favoriteBL = new FavoriteBL();
        private readonly ReactionBL reactionBL = new ReactionBL();


        public async Task<List<PostDto>> GetAllAsync()
        {
            var posts = await postBL.GetAllAsync();
            var postDtos = new List<PostDto>();
            foreach (var post in posts)
            {
                var profile = await profileBL.GetByIdAsync(new Profile { Id = post.User.Id });
                var Allimages = await imageBL.GetAllAsync();
                var images = Allimages.FindAll(i => i.IdPost == post.Id);
                var allfav = await favoriteBL.GetAllAsync();
                var favoriteCount = allfav.Count(f => f.IdPost == post.Id);

                var allreactions = await reactionBL.GetAllAsync();
                var reactionCount = allreactions.Count(r => r.IdPost == post.Id);

                postDtos.Add(new PostDto
                {
                    Title = post.Title,
                    IdUser = post.IdUser,
                    User = post.User,
                    Id = post.Id,
                    UserProfile = profile,
                    Content = post.Content,
                    PostedAt = post.PostedAt,
                    Images = images,
                    FavoriteCount = favoriteCount,
                    ReactionCount = reactionCount
                });
            }
            return postDtos;
        }
    }

    public class PostDto : Post
    {
        public Profile? UserProfile { get; set; }
        public List<Image>? Images { get; set; }
        public int FavoriteCount { get; set; }
        public int ReactionCount { get; set; }

    }
}