using toiec_web.Models;

namespace toiec_web.Repository.IRepository
{
    public interface IQuestionRepository
    {
        Task<IEnumerable<QuestionModel>> GetAllQuestions();
        Task<QuestionModel> GetQuestionById(Guid questionId);
        Task<IEnumerable<QuestionModel>> GetAllQuestionByQuiz(Guid quizId);
        Task<IEnumerable<QuestionModel>> GetAllQuestionByUnit(Guid unitId);
        Task<IEnumerable<QuestionModel>> GetAllQuestionByProfessor(string userId);
        Task<bool> AddQuestion(QuestionModel model, string userId);
        Task<bool> UpdateQuestion(QuestionModel model, Guid questionId, string userId);
        Task<bool> DeleteQuestion(Guid questionId);
    }
}