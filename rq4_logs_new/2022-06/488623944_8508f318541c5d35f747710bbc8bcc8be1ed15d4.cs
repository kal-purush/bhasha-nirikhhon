using System.Data;
using Dapper;
using Microsoft.Data.SqlClient;
using SocialNetwork.Exceptions;
using SocialNetwork.Models.Comments;
using SocialNetwork.Models.Users;

namespace SocialNetwork.Repository;

public class CommentsRepository : ICommentsRepository
{
    private readonly string _connectionString;

    public CommentsRepository(string connectionString)
    {
        _connectionString = connectionString;
    }

    public IEnumerable<Comment> GetAll()
    {
        using (IDbConnection connection = new SqlConnection(_connectionString))
        {
            string commentsQuery = "SELECT * FROM Comments";
            IEnumerable<Comment> comments = connection.Query<Comment>(commentsQuery);

            string userQuery =
                @"SELECT Users.Id, Users.FirstName, Users.SecondName, Users.Email, Users.PasswordHash FROM Users 
                                 INNER JOIN Comments ON Users.Id = Comments.UserId AND Comments.Id = @Id";
            foreach (var comment in comments)
            {
                comment.User = connection.QuerySingle<User>(userQuery, new {Id = comment.Id});
            }
            return comments;
        }
    }

    public IEnumerable<Comment> GetPostsComment(int postId)
    {
        using (IDbConnection connection = new SqlConnection(_connectionString))
        {
            string commentsQuery = "SELECT * FROM Comments WHERE PostId = @postId";
            IEnumerable<Comment> comments = connection.Query<Comment>(commentsQuery, new {postId});

            string userQuery = @"SELECT Users.Id, Users.FirstName, Users.SecondName, Users.Email, Users.PasswordHash FROM Users 
                                 INNER JOIN Comments ON Users.Id = Comments.UserId AND Comments.Id = @Id";
            foreach (var comment in comments)
            {
                comment.User = connection.QuerySingle<User>(userQuery, new {Id = comment.Id});
            }
            return comments;
        }
    }

    public Comment Get(int id)
    {
        using (IDbConnection connection = new SqlConnection(_connectionString))
        {
            string commentsQuery = "SELECT * FROM Comments WHERE Id = @id";
            Comment comment = connection.QueryFirstOrDefault<Comment>(commentsQuery, new {id});

            if (comment == null)
                throw new NotFoundException("Comment not found");

            string userQuery = @"SELECT Users.Id, Users.FirstName, Users.SecondName, Users.Email, Users.PasswordHash FROM Users 
                                 INNER JOIN Comments ON Users.Id = Comments.UserId AND Comments.Id = @Id";
            comment.User = connection.QuerySingle<User>(userQuery, new {Id = comment.Id});
            return comment;
        }
    }

    public void Add(Comment item)
    {
        using (IDbConnection connection = new SqlConnection(_connectionString))
        {
            string sqlQuery = "INSERT INTO Comments VALUES(@Content, @PostId, @UserId) SELECT @@IDENTITY";
            int commentId = connection.QuerySingle<int>(sqlQuery,
                new {Content = item.Content, PostId = item.PostId, UserId = item.User.Id});

            item.Id = commentId;
        }
    }

    public void Update(Comment item)
    {
        using (IDbConnection connection = new SqlConnection(_connectionString))
        {
            string sqlQuery = "UPDATE Comments SET Content = @Content WHERE Id = @Id";
            connection.Query(sqlQuery, item);
        }
    }

    public void Delete(int id)
    {
        using (IDbConnection connection = new SqlConnection(_connectionString))
        {
            string sqlQuery = "DELETE Comments WHERE Id = @Id";
            connection.Query(sqlQuery, new {id});
        }
    }
}