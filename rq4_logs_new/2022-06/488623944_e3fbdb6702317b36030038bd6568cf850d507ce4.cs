using System.Data;
using Dapper;
using Microsoft.Data.SqlClient;
using SocialNetwork.Exceptions;
using SocialNetwork.Models.Posts;

namespace SocialNetwork.Repositories.Posts;

public class PostsRepository : IPostsRepository
{
    private readonly string _connectionString;

    public PostsRepository(string connectionString)
    {
        _connectionString = connectionString;
    }

    public IEnumerable<Post> GetAll()
    {
        using (IDbConnection connection = new SqlConnection(_connectionString))
        {
            IEnumerable<Post> posts = connection.Query<Post>("SELECT * FROM Posts");

            string commentsCountQuery = "SELECT COUNT(*) FROM Comments WHERE PostId = @PostId";
            string likesCountQuery = "SELECT COUNT(*) FROM PostsLikes WHERE PostId = @PostId";
            foreach (var post in posts)
            {
                post.CommentsCount = connection.QuerySingle<int>(commentsCountQuery, new {PostId = post.Id});
                post.LikesCount = connection.QuerySingle<int>(likesCountQuery, new {PostId = post.Id});
            }

            return posts;
        }
    }

    public Post Get(int id)
    {
        using (IDbConnection connection = new SqlConnection(_connectionString))
        {
            Post post = connection.QueryFirstOrDefault<Post>("SELECT * FROM Posts WHERE Id = @id", new {id});

            if (post == null)
                throw new NotFoundException("Post not found");

            string commentsCountQuery = "SELECT COUNT(*) FROM Comments WHERE PostId = @PostId";
            string likesCountQuery = "SELECT COUNT(*) FROM PostsLikes WHERE PostId = @PostId";

            post.CommentsCount = connection.QuerySingle<int>(commentsCountQuery, new {PostId = post.Id});
            post.LikesCount = connection.QuerySingle<int>(likesCountQuery, new {PostId = post.Id});

            return post;
        }
    }

    public void Add(Post item)
    {
        using (IDbConnection connection = new SqlConnection(_connectionString))
        {
            string sqlQuery = "INSERT INTO Posts VALUES (@Content, @DateTime, @CommunityId) SELECT @@IDENTITY";
            item.DateTime = DateTime.Now;
            int postId = connection.QuerySingle<int>(sqlQuery, item);

            item.Id = postId;
        }
    }


    public void Update(Post item)
    {
        using (IDbConnection connection = new SqlConnection(_connectionString))
        {
            string sqlQuery = "UPDATE Posts SET Content = @Content WHERE Id = @Id";
            connection.Query(sqlQuery, item);
        }
    }

    public void Delete(int id)
    {
        using (IDbConnection connection = new SqlConnection(_connectionString))
        {
            connection.Query("DELETE Posts WHERE Id = @Id", new {id});
        }
    }

    public IEnumerable<Post> GetPostsFromCommunity(int communityId)
    {
        using (IDbConnection connection = new SqlConnection(_connectionString))
        {
            IEnumerable<Post> posts = connection.Query<Post>("SELECT * FROM Posts WHERE CommunityId = @communityId",
                new {communityId});

            string commentsCountQuery = "SELECT COUNT(*) FROM Comments WHERE PostId = @PostId";
            string likesCountQuery = "SELECT COUNT(*) FROM PostsLikes WHERE PostId = @PostId";
            foreach (var post in posts)
            {
                post.CommentsCount = connection.QuerySingle<int>(commentsCountQuery, new {PostId = post.Id});
                post.LikesCount = connection.QuerySingle<int>(likesCountQuery, new {PostId = post.Id});
            }

            return posts;
        }
    }

    public bool IsUserLike(int userId, int contentId)
    {
        using (IDbConnection connection = new SqlConnection(_connectionString))
        {
            string sqlQuery = "SELECT * FROM PostsLikes WHERE UserId = @userId AND PostId = @contentId";
            object userLike = connection.QueryFirstOrDefault<object>(sqlQuery, new {userId, contentId});

            return userLike != null;
        }
    }

    public void AddLike(int userId, int contentId)
    {
        using (IDbConnection connection = new SqlConnection(_connectionString))
        {
            string sqlQuery = "INSERT INTO PostsLikes VALUES (@userId, @contentId)";
            connection.Query(sqlQuery, new {userId, contentId});
        }
    }

    public void DeleteLike(int userId, int contentId)
    {
        using (IDbConnection connection = new SqlConnection(_connectionString))
        {
            string sqlQuery = "DELETE PostsLikes WHERE UserId = @userId AND PostId = @contentId";
            connection.Query(sqlQuery, new {userId, contentId});
        }
    }
}