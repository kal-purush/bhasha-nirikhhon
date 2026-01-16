using System.Threading.Tasks;
using AutoMapper;
using CoolBytes.Core.Models;
using CoolBytes.Data;
using CoolBytes.WebAPI.Services;
using MediatR;

namespace CoolBytes.WebAPI.Features.BlogPosts
{
    public class AddBlogPostCommandHandler : IAsyncRequestHandler<AddBlogPostCommand, BlogPostViewModel>
    {
        private readonly AppDbContext _appDbContext;
        private readonly IUserService _userService;

        public AddBlogPostCommandHandler(AppDbContext appDbContext, IUserService userService)
        {
            _appDbContext = appDbContext;
            _userService = userService;
        }

        public async Task<BlogPostViewModel> Handle(AddBlogPostCommand message)
        {
            var user = await _userService.GetUser();
            var author = _appDbContext.Authors.Find(message.AuthorId);
            var blogPost = new BlogPost(message.Subject, message.ContentIntro, message.Content, author);

            if (message.BlogPostTags != null)
            {
                blogPost.AddTags(message.BlogPostTags);
            }

            await _appDbContext.SaveChangesAsync();

            return Mapper.Map<BlogPostViewModel>(blogPost);
        }
    }
}