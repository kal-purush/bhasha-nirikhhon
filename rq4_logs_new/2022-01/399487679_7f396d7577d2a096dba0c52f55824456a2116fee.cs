using System.Text.Json.Serialization;

namespace Igtampe.Neco.Common {

    /// <summary></summary>
    public class Notification : AutomaticallyGeneratableIdentifiable {

        /// <summary>Text of this notification</summary>
        public string Text { get; set; } = "";

        /// <summary>Date and Time this notification was sent</summary>
        public DateTime Date { get; set; } = DateTime.Now;

        /// <summary>User that this notification belongs to</summary>
        [JsonIgnore]
        public User? User { get; set; }
    }
}