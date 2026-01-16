using System.Globalization;
using System.Xml;

namespace Sharp.Xmpp.Extensions
{
    /// <summary>
    /// Implements MUC Mediated Invitation as described in XEP-0045.
    /// (Please note: server responses have different properties filled in compared to client requests)
    /// </summary>
    public interface IInvite
    {
        /// <summary>
        /// The ID of the stanza, which may be used for internal tracking of stanzas.
        /// </summary>
        string Id { get; set; }

        /// <summary>
        /// Specifies the JID of the intended recipient for the stanza.
        /// </summary>
        Jid To { get; set; }

        /// <summary>
        /// Specifies the JID of the sender. If this is null, the stanza was generated
        /// by the client's server.
        /// </summary>
        Jid From { get; set; }

        /// <summary>
        /// JID of the user the invite is intended to be send to.
        /// </summary>
        Jid SendTo { get; set; }

        /// <summary>
        /// JID of the user the invite has been sent from.
        /// </summary>
        Jid ReceivedFrom { get; }
        
        /// <summary>
        /// Custom message that may be sent with the invitation.
        /// </summary>
        string Reason { get; set; }

        /// <summary>
        /// (Optional) password of the chat room in the invitation.
        /// </summary>
        string Password { get; set; }
        
        /// <summary>
        /// The data of the stanza.
        /// </summary>
        XmlElement Data { get; }
        
        /// <summary>
        /// Determines whether the stanza is empty, i.e. has no child nodes.
        /// </summary>
        bool IsEmpty { get; }

        /// <summary>
        /// The language of the XML character data if the stanza contains data that is
        /// intended to be presented to a human user.
        /// </summary>
        CultureInfo Language { get; set; }
    }
}