using DemocracyBot.Data.Schemas;

namespace DemocracyBot.Data.Storage; 

public class FileStorageService : IStorageService {
    
    public void Init() {
        throw new NotImplementedException();
    }

    public void Deinit() {
        throw new NotImplementedException();
    }

    public void RegisterVote(long user, long vote) {
        throw new NotImplementedException();
    }

    public Term GetCurrentTerm() {
        throw new NotImplementedException();
    }

    public void SetCurrentTerm(Term term) {
        throw new NotImplementedException();
    }

    public Poll GetCurrentPoll() {
        throw new NotImplementedException();
    }

    public void StartNewPoll() {
        throw new NotImplementedException();
    }

    public void EndPoll(out long winner, out int votes) {
        throw new NotImplementedException();
    }
    
}