using System;
using System.ComponentModel.DataAnnotations.Schema;

namespace BriefAssistant.Data
{
    public class InitialBriefDto : IUserData
    {
        public Guid Id { get; set; }
        public Guid ApplicationUserId { get; set; }
        public string IssuesPresented { get; set; }
        public string OralArgumentStatement { get; set; }
        public string PublicationStatement { get; set; }
        public string CaseFactsStatement { get; set; }
        public string AppendixDocuments { get; set; }
    }
}