using DB.Core.Entities.Identity;
using System.Collections.Generic;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace DB.Core.Entities.Chat
{
    public class ChatUserEntity : BaseEntity
    {
        [Required]
        public int ChatRoomId { get; set; }

        [Required]
        public int UserId { get; set; }

        [ForeignKey(nameof(ChatRoomId))]
        public ChatRoomEntity ChatRoom { get; set; }

        [ForeignKey(nameof(UserId))]
        public UserEntity User { get; set; }

        public List<ChatMessageEntity> ChatMessages { get; set; }
    }
}