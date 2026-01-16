using SocialNetwork.Models.Images;

namespace SocialNetwork.Repository;

public interface IImagesRepository : IRepository<Image, int>
{
    public Image? GetUserAvatar(int userId);
    public IEnumerable<Image> GetUserPhotos(int userId);
    public void SetDefaultValueForUserAvatar(int userId);
    public void UpdateUserAvatar(int userId, int imageId);
    public void AddPhotoToUser(int userId, int imageId);
    public void DeletePhotoFromUser(int userId, int imageId);

    public Image? GetCommunityAvatar(int communityId);
    public void SetDefaultValueForCommunityAvatar(int communityId);
    public void UpdateCommunityAvatar(int communityId, int imageId);

    public IEnumerable<Image> GetPostImages(int postId);
    public void AddImageToPost(int postId, int imageId);

}