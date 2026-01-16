namespace GoF.CasinoCraps
{
    using System;
    using System.Collections.Generic;
    using System.Linq;

    /// <summary>
    /// Thrown when an incorrect amount is entered for a bet.
    /// </summary>
    [Serializable]
    public class BetAmountException : CrapsException
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="BetAmountException"/> class.
        /// </summary>
        public BetAmountException()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BetAmountException"/> class.
        /// </summary>
        /// <param name="message">The error message.</param>
        public BetAmountException(string message)
            : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BetAmountException"/> class.
        /// </summary>
        /// <param name="message">The error message.</param>
        /// <param name="innerException">The inner exception.</param>
        public BetAmountException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="BetAmountException"/> class.
        /// </summary>
        /// <param name="info">The serialization info.</param>
        /// <param name="context">The streaming context.</param>
        protected BetAmountException(System.Runtime.Serialization.SerializationInfo info, System.Runtime.Serialization.StreamingContext context)
            : base(info, context)
        {
        }
    }
}