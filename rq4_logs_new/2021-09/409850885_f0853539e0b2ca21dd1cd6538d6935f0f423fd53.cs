using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace ChatService
{
    public class NodeDssMessage
    {
        public const string IceSeparatorChar = "|";
        public enum Type
        {
            /// <summary>
            /// An unrecognized message.
            /// </summary>
            Unknown = 0,

            /// <summary>
            /// A SDP offer message.
            /// </summary>
            Offer,

            /// <summary>
            /// A SDP answer message.
            /// </summary>
            Answer,

            /// <summary>
            /// A trickle-ice or ice message.
            /// </summary>
            Ice
        }

        /// <summary>
        /// The message type.
        /// </summary>
        public Type MessageType = Type.Unknown;

        /// <summary>
        /// The primary message contents.
        /// </summary>
        public string Data;

        /// <summary>
        /// The data separator needed for proper ICE serialization.
        /// </summary>
        public string IceDataSeparator;
    }
}