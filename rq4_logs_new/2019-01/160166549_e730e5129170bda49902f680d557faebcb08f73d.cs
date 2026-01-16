//-----------------------------------------------------------------------
// <copyright file="TaskNotFoundException.cs" company="SoftServe">
//     Company copyright tag.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Runtime.Serialization;

namespace GtdCommon.Exceptions
{
    /// <summary>
    /// class for custom Import Error Exception
    /// </summary>
    public class ImportErrorException : Exception
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ImportErrorException" /> class.
        /// </summary>
        public ImportErrorException() : base("Error encountered during import!")
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ImportErrorException" /> class.
        /// </summary>
        /// <param name="message">string message</param>
        public ImportErrorException(string message) : base(message)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ImportErrorException" /> class.
        /// </summary>
        /// <param name="message">string message</param>
        /// <param name="inner">inner exception</param>
        public ImportErrorException(string message, Exception inner) : base(message, inner)
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="ImportErrorException" /> class.
        /// </summary>
        /// <param name="info">serialization info</param>
        /// <param name="context">streaming context</param>
        protected ImportErrorException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
            this.ResourceReferenceProperty = info.GetString("ResourceReferenceProperty");
        }

        /// <summary>
        /// Gets or sets value of resource reference property
        /// </summary>
        public string ResourceReferenceProperty { get; set; }
    }
}