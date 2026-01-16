using Quiz.Application.Repositories;
using Quiz.Domain.Models;

namespace Quiz.Application.Services
{
    using Quiz = Domain.Models.Quiz;

    public class QuizService : IQuizService
    {
        private readonly IQuizRepository _repository;
        private readonly IQuestionRepository _questionRepository;
        private readonly IAnswerRepository _answerRepository;

        public QuizService(IQuizRepository repository, IQuestionRepository questionRepository, IAnswerRepository answerRepository)
        {
            _repository = repository;
            _questionRepository = questionRepository;
            _answerRepository = answerRepository;
        }
        public async Task<IEnumerable<Quiz>> Get()
        {
            var quizzes = await _repository.Get();

            return quizzes;
        }

        public async Task<Quiz> GetById(int id)
        {
            var quiz = await _repository.GetById(id);

            var questions = await _questionRepository.GetQuestionsByQuizId(id);

            var answers = await _answerRepository.GetAnswersByQuizId(id);

            return new Quiz
            {
                Id = quiz.Id,
                Title = quiz.Title,
                Questions = questions.Select(x => new Question
                {
                    Id = x.Id,
                    Text = x.Text,
                    CorrectAnswerId = x.CorrectAnswerId,
                    Answers = answers.Where(y => y.QuestionId == x.Id)
                        .Select(y => new Answer
                        {
                            Id = y.Id,
                            Text = y.Text,
                            QuestionId = y.QuestionId
                        })
                })
            };
        }

        public async Task<IEnumerable<Question>> GetQuestionsByQuizId(int quizId)
        {
            var questions = await _questionRepository.GetQuestionsByQuizId(quizId);

            return questions;
        }

        public async Task<IEnumerable<Answer>> GetAnswersByQuizId(int quizId)
        {
            var answers = await _answerRepository.GetAnswersByQuizId(quizId);

            return answers;
        }
    }
}