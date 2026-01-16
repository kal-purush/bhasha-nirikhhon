using System;
using SchedulearnBackend.Models;

namespace SchedulearnBackend.Controllers.DTOs
{
    public class CreateNewSuggestion
    {
        public int TopicId { get; set; }
        public int SuggesterId { get; set; }
        public int SuggesteeId { get; set; }

        public Suggestion CreateSuggestion()
        {
            return new Suggestion()
            {
                TopicId = TopicId,
                SuggesterId = SuggesterId,
                SuggesteeId = SuggesteeId,
                CreationDate = DateTime.Now,
            };
        }
    }
}