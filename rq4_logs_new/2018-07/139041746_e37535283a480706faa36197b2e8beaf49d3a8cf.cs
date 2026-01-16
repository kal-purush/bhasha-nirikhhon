namespace Retrospective.Events
{
    public class Sync_AskForSync
    {
        public const string EventName = "Sync_AskForSync";

        public string AskForSyncClientId { get; set; }

        public bool HasMaster { get; set; } = false;
    }
}