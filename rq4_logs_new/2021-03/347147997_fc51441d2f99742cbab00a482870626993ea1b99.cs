using MarisDoodleLibrary.Models;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DoodleWebMvc.Models
{
    public class PollResultsForDisplay
    {
        public PollVotingResultModel PollResults { get; set; }
        public List<string> VoterNamesPerOption { get; set; } = new List<string>();
    }
}