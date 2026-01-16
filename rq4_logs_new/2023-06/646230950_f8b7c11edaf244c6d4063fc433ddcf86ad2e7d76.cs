namespace Blog.Api.Application.Commands
{
    [DataContract]
    public class AddPostCommand : IRequest<Guid>, IRequest<bool>
    {
        [DataMember] public string Title { get; private set; }
        [DataMember] public string Content { get; private set; }
        [DataMember] public string ImageURL { get; private set; }

        public AddPostCommand(string title, string content, string imageUrl)
        {
            Title = title;
            Content = content;
            ImageURL = imageUrl;
        }
    }
}