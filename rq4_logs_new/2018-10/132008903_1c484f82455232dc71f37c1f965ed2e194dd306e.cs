using Newtonsoft.Json;

namespace ESFA.DC.JobScheduler.Settings
{
    public class EasMessageTopics
    {
        [JsonRequired]
        public string TopicProcessing { get; set; }
    }
}