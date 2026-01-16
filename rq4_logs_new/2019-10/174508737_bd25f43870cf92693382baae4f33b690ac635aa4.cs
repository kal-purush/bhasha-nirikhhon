using System;
using System.Collections.Generic;
using System.Text;
using Nexus.Link.Libraries.Core.Logging;
using Nexus.Link.Services.Contracts.Events;

namespace Nexus.Link.Services.Contracts
{
    public static class ContractExtensions
    {
        /// <summary>
        /// Implements <see cref="ILoggable"/> for <see cref="IPublishableEvent"/>
        /// </summary>
        public static string ToLogString(this IPublishableEvent item)
        {
            return item.Metadata == null
                ? "Event without Metadata!" 
                : item.Metadata.ToLogString();
        }

        /// <summary>
        /// Implements ToString() for <see cref="IPublishableEvent"/>
        /// </summary>
        public static string ToString(this IPublishableEvent item)
        {
            return item.Metadata == null
                ? "Event without Metadata!" 
                : item.Metadata.ToString();
        }
    }
}