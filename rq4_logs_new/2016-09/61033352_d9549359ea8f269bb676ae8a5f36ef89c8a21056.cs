using System;
using System.Collections.Generic;
using Sharp.Xmpp.Im;

namespace Sharp.Xmpp.Extensions
{
    public interface IMultiUserChat
    {
        /// <summary>
        /// The event that is raised when an invite to a group chat is received.
        /// </summary>
        event EventHandler<GroupInviteEventArgs> InviteReceived;

        /// <summary>
        /// The event that is raised when an invite to a group chat is declined.
        /// </summary>
        event EventHandler<GroupInviteDeclinedEventArgs> InviteDeclined;

        /// <summary>
        /// The event that is raised when the server responds with an error in relation to a group chat.
        /// </summary>
        event EventHandler<GroupErrorEventArgs> ErrorResponse;

        /// <summary>
        /// The event that is raised when a group chat raises a MucStatus about a participant.
        /// </summary>
        event EventHandler<GroupPresenceEventArgs> MucStatus;

        /// <summary>
        /// The event that is raised when the subject is changed in a group chat.
        /// </summary>
        event EventHandler<MessageEventArgs> SubjectChanged;
        
        /// <summary>
        /// Responds to a group chat invitation with a decline message.
        /// </summary>
        /// <param name="invite">Original group chat invitation.</param>
        /// <param name="reason">Reason for declining.</param>
        void DeclineInvite(IInvite invite, string reason);

        /// <summary>
        /// Allows owners to destroy the room.
        /// </summary>
        bool DestroyRoom(Jid room, string reason = null);

        /// <summary>
        /// Returns a list of active public chat room messages.
        /// </summary>
        /// <param name="chatService">JID of the chat service (depends on server)</param>
        /// <returns>List of Room JIDs</returns>
        IEnumerable<RoomInfoBasic> DiscoverRooms(Jid chatService);

        /// <summary>
        /// Allows moderators (and above) to edit the room subject.
        /// </summary>
        void EditRoomSubject(Jid room, string subject);

        /// <summary>
        /// Requests a list of occupants within a specific room.
        /// </summary>
        IEnumerable<Occupant> GetMembers(Jid room);

        /// <summary>
        /// Requests a list of occupants with a specific role.
        /// </summary>
        IEnumerable<Occupant> GetMembers(Jid room, Role role);

        /// <summary>
        /// Requests a list of occupants with a specific affiliation.
        /// </summary>
        IEnumerable<Occupant> GetMembers(Jid room, Affiliation affiliation);

        /// <summary>
        /// Requests previous chat room messages.
        /// </summary>
        void GetMessageLog(History options);

        /// <summary>
        /// Returns a list of active public chat room messages.
        /// </summary>
        /// <param name="chatRoom">Existing room info</param>
        /// <returns>Information about room</returns>
        RoomInfoExtended GetRoomInfo(Jid chatRoom);

        /// <summary>
        /// Joins or creates new room using the specified room.
        /// </summary>
        /// <param name="jid">Chat room</param>
        /// <param name="nickname">Desired nickname</param>
        /// <param name="password">(Optional) Password</param>
        void JoinRoom(Jid jid, string nickname, string password = null);

        /// <summary>
        /// Allows moderators to kick an occupant from the room.
        /// </summary>
        /// <param name="room">chat room</param>
        /// <param name="nickname">user to kick</param>
        /// <param name="reason">reason for kick</param>
        void KickOccupant(Jid room, string nickname, string reason);

        /// <summary>
        /// Leaves the specified room.
        /// </summary>
        void LeaveRoom(Jid jid, string nickname);
        void ModifyRoomConfig(Jid room, RegistrationCallback callback);

        /// <summary>
        /// Allows occupants to request privileges to a room.
        /// </summary>
        void RequestPrivilige(Jid room, Role role);
        void RequestRegistration(Jid room);

        /// <summary>
        /// Asks the chat service to invite the specified user to the chat room you specify.
        /// </summary>
        /// <param name="to">user you intend to invite to chat room.</param>
        /// <param name="message">message you want to send to the user.</param>
        /// <param name="room">Jid of the chat room.</param>
        /// <param name="password">Password if any.</param>
        void SendInvite(Jid to, Jid room, string message, string password = null);

        /// <summary>
        /// Set your nickname in the room.
        /// </summary>
        void SetNickName(Jid room, string nickname);

        /// <summary>
        /// Allows owners and admins to grant privileges to an occupant.
        /// </summary>
        bool SetPrivilege(Jid room, string nickname, Role privilege, string reason = null);

        /// <summary>
        /// Allows owners and admins to grant privileges to an occupant.
        /// </summary>
        bool SetPrivilege(Jid room, string nickname, Affiliation privilege, string reason = null);

        /// <summary>
        /// Sets your presence in the specified room.
        /// </summary>
        void SetStatusInRoom(Jid roomWithNick, Availability availability);
    }
}