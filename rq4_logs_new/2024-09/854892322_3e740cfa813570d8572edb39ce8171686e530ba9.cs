using Microsoft.EntityFrameworkCore;
using Rare.Models;

namespace Rare_BE.APIs
{
    public class PostTagAPI
    {
        public static void Map(WebApplication app)
        {
            app.MapPost("/posts/{postId}/add-tag/{tagId}", (RareDbContext db, int postId, int tagId) =>
            {
                Post? post = db.Posts
                    .Include(post => post.Tags)
                    .FirstOrDefault(post => post.Id == postId);

                if (post == null)
                {
                    return Results.NotFound("Invalid post Id");
                }

                Tag? tag = db.Tags.FirstOrDefault(tag => tag.Id == tagId);

                if (tag == null)
                {
                    return Results.NotFound("Invalid tag Id");
                }
                else if (post.Tags.Contains(tag))
                {
                    return Results.Ok("Post already has tag");
                }

                post.Tags.Add(tag);
                db.SaveChanges();
                return Results.Ok("Tag added");
            });

            app.MapDelete("/posts/{postId}/remove-tag/{tagId}", (RareDbContext db, int postId, int tagId) =>
            {
                Post? post = db.Posts
                    .Include(post => post.Tags)
                    .FirstOrDefault(post => post.Id == postId);

                if (post == null)
                {
                    return Results.NotFound("Invalid post Id");
                }

                Tag? tag = db.Tags.FirstOrDefault(tag => tag.Id == tagId);

                if (tag == null)
                {
                    return Results.NotFound("Invalid tag Id");
                }
                else if (!post.Tags.Contains(tag))
                {
                    return Results.Ok("Post does not have tag");
                }

                post.Tags.Remove(tag);
                db.SaveChanges();
                return Results.Ok("Tag removed");
            });
        }
    }
}