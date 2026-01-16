using AskMe.Data.Entities;
using AskMe.Data.Models.Answers;
using AskMe.Data.Models.Questions;
using AskMe.Data.Models.Sets;
using AskMe.Data.Models.Themes;

namespace AskMe.Services.Utilities
{
    public static class DataConverter
    {
        public static SetDto SetToDto(Set set) => new SetDto
        {
            Id = set.Id,
            Name = set.Name,
            Description = set.Description,
            Themes = set.Themes
             .Select(ThemeToDto)
             .ToList()
        };
        public static ThemeDto ThemeToDto(Theme theme) => new ThemeDto
        {
            Id = theme.Id,
            Name = theme.Name,
            Description = theme.Description,
            SetId = theme.SetId,
            Questions = theme.Questions
            .Select(QuestionToDto)
            .ToList()
        };
        public static QuestionDto QuestionToDto(Question question) => new QuestionDto
        {
            Id = question.Id,
            Text = question.Text,
            ThemeId = question.ThemeId,
            Answers = question.Answers
            .Select(AnswerToDto)
            .ToList()
        };
        public static AnswerDto AnswerToDto(Answer answer) => new AnswerDto
        {
            Id = answer.Id,
            Text = answer.Text,
            QuestionId = answer.QuestionId
        };
    }
}