namespace TheReplacement.PTA.Common.Models
{
    /// <summary>
    /// Represents a message in Pokemon Tabletop Adventures 
    /// </summary>
    public class UserMessageModel
    {
        /// <summary>
        /// User that sent the message
        /// </summary>
        public string User { get; set; }

        /// <summary>
        /// Contents of what was sent
        /// </summary>
        public string Message { get; set; }

        /// <summary>
        /// Game the message is associated with
        /// </summary>
        public string Game { get; set; }

        /// <summary>
        /// Timestamp for when the message was created
        /// </summary>
        public string Timestamp { get; set; }
    }
}