using DoodleWebMvc.Models;
using MarisDoodleLibrary.Contracts.Routines;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace DoodleWebMvc.Utils
{
    public class ModelPopulator : IModelPopulator
    {
        private readonly IPollRoutine _pollRoutine;

        public ModelPopulator(IPollRoutine pollRoutine)
        {
            _pollRoutine = pollRoutine;
        }

        public async Task<PollFullModel> PopulatePollAndOptionsForDisplay(int pollId)
        {
            PollFullModel model = new PollFullModel();
            model.Poll = await _pollRoutine.GetBasicPoll(pollId);
            model.Options = await _pollRoutine.GetPollOptionsForDisplay(pollId);

            return model;
        }

        public async Task<PollFullVotingModel> PopulatePollAndOptionsForVoting(int pollId)
        {
            PollFullVotingModel model = new PollFullVotingModel();
            model.Poll = await _pollRoutine.GetBasicPoll(pollId);
            model.Options = await _pollRoutine.GetPollOptionsForVoting(pollId);

            return model;
        }
    }
}