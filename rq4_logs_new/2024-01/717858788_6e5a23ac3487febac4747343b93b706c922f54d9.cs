using QuizzenApp.Domain.Entities.QuestionAggregate;

namespace QuizzenApp.Domain.Entities.ExamAggregate;

public class Subject
{
    public Guid Id { get; set; }
    public string Name { get; set; }
    public Guid ExamId { get; set; }
    public Exam Exam { get; set; }
    public ICollection<Topic> Topics;
    public ICollection<Question> Questions;

    public Subject()
    {

    }

    public Subject(Guid id, string name, Guid examId)
    {
        Id = id;
        Name = name;
        ExamId = examId;
    }
}