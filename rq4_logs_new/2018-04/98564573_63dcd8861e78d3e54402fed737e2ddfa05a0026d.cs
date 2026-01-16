namespace Lykke.Messages.Email.MessageData
{
    public class SwiftCashoutRequestedData : IEmailMessageData
    {
        public const string QueueName = "SwiftCashoutRequested";

        public string FullName { get; set; }
    }
}