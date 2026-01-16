using MovieFiles.Api.Client.Mappers;
using MovieFiles.Core.Interfaces;

namespace MovieFiles.Api.Client.Services
{
    internal class CommentService : BaseService, ICommentService
    {
        public CommentService(string httpUrl, string functionAppKey) : base(httpUrl, functionAppKey)
        {
        }

        public async Task Comment(Core.Models.Comment comment, int movieId, Guid userId)
        {
            await _client.CommentAsync(movieId, userId, _functionAppKey, UiToClient.Map(comment));
        }

        public async Task<List<Core.Models.Comment>> GetComments(int movieId, int page)
        {
            try
            {
                return (await _client.GetCommentsAsync(movieId, page, _functionAppKey))
                    .Select(ClientToUi.Map)
                    .ToList();
            }
            catch (ApiException ex)
            {
                //No comments found for a movie
                return new List<Core.Models.Comment>();
            }
        }
    }
}