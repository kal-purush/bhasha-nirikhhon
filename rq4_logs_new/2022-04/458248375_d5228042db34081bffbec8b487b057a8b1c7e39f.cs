using System;
using System.ComponentModel.DataAnnotations;
using System.ComponentModel.DataAnnotations.Schema;

namespace Meetins.Models.Entities
{
    /// <summary>
    /// Класс соответствует таблице Events.KudagoInvites.
    /// </summary>
    [Table("KudagoInvites", Schema = "Events")]
    public class KudagoInvites
    {
        /// <summary>
        /// PK.
        /// </summary>
        [Key]
        [Column("InviteId", TypeName = "uuid")]
        public Guid InviteId { get; set; }

        /// <summary>
        /// Идентификатор события на KudaGo.
        /// </summary>
        [Column("KudagoEventId", TypeName = "bigint")]
        public long KudagoEventId { get; set; }

        /// <summary>
        /// Дата приглашения.
        /// </summary>
        [Column("CreatedAt", TypeName = "timestamp")]
        public DateTime CreatedAt { get; set; }

        /// <summary>
        /// Комментарий.
        /// </summary>
        [Column("Comment", TypeName = "text")]
        public string Comment { get; set; }

        /// <summary>
        /// Пользователь, от которого приглашение.
        /// </summary>
        [ForeignKey("KudagoInvites_UserIdFrom_fkey")]
        [Column("UserIdFrom", TypeName = "uuid")]
        public Guid UserIdFrom { get; set; }

        /// <summary>
        /// Пользователь, которому приглашение.
        /// </summary>
        [ForeignKey("KudagoInvites_UserIdTo_fkey")]
        [Column("UserIdTo", TypeName = "uuid")]
        public Guid UserIdTo { get; set; }

        /// <summary>
        /// Просмотрено ли приглашение.
        /// </summary>
        [Column("IsViewed", TypeName = "boolean")]
        public bool IsViewed { get; set; }
    }
}