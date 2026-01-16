using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Identity;
using Microsoft.AspNetCore.Mvc;
using Microsoft.EntityFrameworkCore;
using server.Data;
using server.Entities;

namespace server.Controllers;

public class LikeController : BaseApiController
{
    private readonly StoreContext _context;
    private readonly UserManager<User> _userManager;

    public LikeController(StoreContext context, UserManager<User> userManager)
    {
        _context = context;
        _userManager = userManager;
    }

    [Authorize]
    [HttpPost]
    public async Task LikePost(int eventId)
    {
        // Get the current user (you may need to implement user authentication)
        var user = await _userManager.FindByNameAsync(User.Identity.Name);
        var userId = user.Id;


        // Check if the user has already liked the post
        var existingLike = _context.Like.Where(entity => entity.UserId == userId && entity.EventId == eventId)
            .FirstOrDefault();

        if (existingLike != null)
        {
            // User has already liked the post, you may want to handle this scenario
            // For now, let's remove the like (unlike)
            _context.Like.Remove(existingLike);
        }
        else
        {
            // User hasn't liked the post, so add a new like
            var newLike = new Like
            {
                UserId = userId,
                EventId = eventId
            };

            _context.Like.Add(newLike);
        }

        _context.SaveChanges();
    }
    
    [Authorize]
    [HttpPost("getEvents")]
    public async Task<List<Event>> FetchLikedPosts()
    {
        // Get the current user (you may need to implement user authentication)
        var user = await _userManager.FindByNameAsync(User.Identity.Name);
        var userId = user.Id;

        return  await _context.Like.Where(u => u.UserId == userId)
            .Include(e => e.Event)
            .Select(e => e.Event)
            .ToListAsync();
    }
}