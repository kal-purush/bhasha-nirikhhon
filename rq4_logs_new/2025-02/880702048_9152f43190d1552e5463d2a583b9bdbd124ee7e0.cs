using CSharpFunctionalExtensions;
using P2Project.SharedKernel.Errors;

namespace P2Project.VolunteerRequests.Domain.ValueObjects
{
    public class RejectionComment
    {
        private RejectionComment() { }
        private RejectionComment(string comment)
        {
            Comment = comment;
        }
        public string Comment { get; } = default!;

        public static Result<RejectionComment, Error> Create(string comment)
        {
            if (string.IsNullOrWhiteSpace(comment))
                return Errors.General.ValueIsInvalid(nameof(Comment));

            var newComment = new RejectionComment(comment);

            return newComment;
        }
    }
}