namespace Messaging.Buffer.Exceptions
{
    /// <summary>
    /// Exception for forbidden subscription
    /// </summary>
    public class SubscriptionException : Exception
    {
        /// <summary>
        /// Ctor
        /// </summary>
        public SubscriptionException()
        {
        }

        /// <summary>
        /// Ctor
        /// </summary>
        /// <param name="message"></param>
        public SubscriptionException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Ctor
        /// </summary>
        /// <param name="message"></param>
        /// <param name="inner"></param>
        public SubscriptionException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}