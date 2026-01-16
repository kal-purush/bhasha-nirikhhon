namespace BoardGames.Services.Data
{
    using BoardGames.Services.Data.Contracts;
    using System.Linq;
    using BoardGames.Data.Models;
    using BoardGames.Data.Common;

    public class CommentsService : ICommentsService
    {
        private readonly IDbRepository<Comment> comments;

        public CommentsService(IDbRepository<Comment> comments)
        {
            this.comments = comments;
        }

        public void Add(string content, string authorId, int tutorialId)
        {
            var newComment = new Comment
            {
                Content = content,
                AuthorId = authorId,
                TutorialId = tutorialId
            };

            this.comments.Add(newComment);
            this.comments.Save();
        }

        public void Delete(int id)
        {
            var commentToBeDeleted = this.comments.GetById(id);

            this.comments.HardDelete(commentToBeDeleted);
            this.comments.Save();
        }

        public void Edit(int id, string content)
        {
            var commentToBeEdited = this.comments.GetById(id);

            commentToBeEdited.Content = content;
            this.comments.Save();
        }

        public IQueryable<Comment> GetAll()
        {
            return this.comments.All();
        }
    }
}