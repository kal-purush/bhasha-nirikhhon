using Microsoft.AspNetCore.SignalR;
using System.Security;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using DAL;
using DAL.Entities;
using Microsoft.Extensions.Hosting;

namespace ProgrammingForum_ASPNETCore.Hubs
{
    public class PostHub : Hub
    {
        private readonly AppDbContext _context;

        public PostHub(AppDbContext context)
        {
            _context = context;
        }

        public async Task UpdateLikes(int postId, string userId)
        {
            bool liked;
            //add or remove like to db (remove if exists)
            if (_context.Likes.Find(userId, postId) != null)
            {
                Like like = await _context.Likes.FindAsync(userId, postId);
                _context.Likes.Remove(like);
                _context.SaveChanges();

                //update likesCount in Posts table
                Post post = await _context.Posts.FindAsync(postId);
                post.LikesCount -= 1;
                await _context.SaveChangesAsync();

                liked = false;
            }
            else
            {
                Like like = new Like { UserId = userId, PostId = postId, Date = DateTime.Now };
                await _context.Likes.AddAsync(like);
                await _context.SaveChangesAsync();

                //update likesCount in Posts table
                Post post = await _context.Posts.FindAsync(postId);
                post.LikesCount += 1;
                await _context.SaveChangesAsync();
                liked = true;
            }

            int totalLikes = _context.Likes.Where(p => p.PostId == postId).Count();

            await Clients.All.SendAsync("UpdateLikesInPage", totalLikes, postId, liked, userId);
        }
    }
}