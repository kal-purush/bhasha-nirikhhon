//-----------------------------------------------------------------------
// <copyright file="PasswordMismatchException.cs" company="SoftServe">
//     Company copyright tag.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Runtime.Serialization;

namespace GtdCommon.Exceptions
{
    /// <summary>
    /// class for custom Password Mismatch Exception
    /// </summary>
    public class PasswordMismatchException : Exception
    {
        /// <summary>
        /// Exception error message
        /// </summary>
        private const string PasswordMismatchExceptionMessage = "Incorrect password entered";

        /// <summary>
        /// Initializes a new instance of the <see cref="PasswordMismatchException" /> class.
        /// </summary>
        public PasswordMismatchException() : base(PasswordMismatchExceptionMessage)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PasswordMismatchException" /> class.
        /// </summary>
        /// <param name="message">string message</param>
        public PasswordMismatchException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PasswordMismatchException" /> class.
        /// </summary>
        /// <param name="message">string message</param>
        /// <param name="innerException">inner exception</param>
        public PasswordMismatchException(string message, Exception innerException) : base(message, innerException)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="PasswordMismatchException" /> class.
        /// </summary>
        /// <param name="info">serialization info</param>
        /// <param name="context">streaming context</param>
        protected PasswordMismatchException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}