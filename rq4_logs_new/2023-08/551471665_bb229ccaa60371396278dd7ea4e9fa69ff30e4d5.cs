using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace LightBlueFox.Connect.Net
{
    /// <summary>
    /// The underlying network protocol on the connection socket.
    /// Limited subset of <see cref="System.Net.Sockets.ProtocolType"/>
    /// </summary>
    public enum SocketProtocol
    {
        /// <summary>
        /// The Transmition Control Protocol guarantees packet arrival and preserves order at the expense of some network speed.
        /// </summary>
        TCP = 6,
        /// <summary>
        /// The User Datagram Protocol achieves high speeds, but sent packets might arrive in different order or not at all.
        /// </summary>
        UDP = 17,
    }
}