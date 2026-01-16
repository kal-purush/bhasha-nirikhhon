using System;
using SchedulearnBackend.Models;

namespace SchedulearnBackend.Controllers.DTOs
{
    public class SuggestionForUser
    {
        public int topicId;
        public string topicName;

        public int suggesterId;
        public string suggesterName;
        public string suggesterSurname;

        public DateTime createdDate;

        public SuggestionForUser(Suggestion suggestion)
        {
            topicId = suggestion.Topic.Id;
            topicName = suggestion.Topic.Name;

            suggesterId = suggestion.Suggester.Id;
            suggesterName = suggestion.Suggester.Name;
            suggesterSurname = suggestion.Suggester.Surname;

            createdDate = suggestion.CreationDate;
        }
    }
}