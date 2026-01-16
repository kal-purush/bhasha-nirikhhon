using Application.CQRS.Posts.Responses;
using Application.Persistance.Interfaces.AccountInterfaces;
using Application.Persistance.Interfaces.PostsInterfaces;
using MediatR;

namespace Application.CQRS.Posts.Commands.LikePosts
{
    public sealed class LikePostCommandHandler : IRequestHandler<LikePostCommand, LikePostResponse>
    {
        private readonly IPostsRepository _postsRepository;
        private readonly ICurrentUserService _currentUserService;

        public LikePostCommandHandler(IPostsRepository postsRepository, ICurrentUserService currentUserService)
        {
            _postsRepository = postsRepository;
            _currentUserService = currentUserService;
        }

        public async Task<LikePostResponse> Handle(LikePostCommand request, CancellationToken cancellationToken)
        {
            var userId = _currentUserService.UserId;
            var likes = await _postsRepository.LikeSystemAsync(request.PostId, userId, cancellationToken);

            return new LikePostResponse(likes);
        }
    }
}