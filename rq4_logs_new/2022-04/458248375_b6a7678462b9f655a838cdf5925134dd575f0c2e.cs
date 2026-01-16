using System;

namespace Meetins.Models.KudaGo
{
    /// <summary>
    /// Выходная модель приглашений.
    /// </summary>
    public class KudagoInvitesOutput
    {
        /// <summary>
        /// PK.
        /// </summary>
        public Guid InviteId { get; set; }

        /// <summary>
        /// Идентификатор события на KudaGo.
        /// </summary>        
        public long KudagoEventId { get; set; }

        /// <summary>
        /// Дата приглашения.
        /// </summary>        
        public DateTime CreatedAt { get; set; }

        /// <summary>
        /// Комментарий.
        /// </summary>        
        public string Comment { get; set; }

        /// <summary>
        /// Пользователь, от которого приглашение.
        /// </summary>        
        public Guid UserIdFrom { get; set; }

        /// <summary>
        /// Имя пользователя, от которого приглашение.
        /// </summary>
        public string UserNameFrom { get; set; }

        /// <summary>
        /// Аватар пользователя, от которого приглашение.
        /// </summary>
        public string UserAvatarFrom { get; set; }

        /// <summary>
        /// Пользователь, которому приглашение.
        /// </summary>        
        public Guid UserIdTo { get; set; }

        /// <summary>
        /// Имя пользователя, которому приглашение.
        /// </summary>
        public string UserNameTo { get; set; }

        /// <summary>
        /// Аватар пользователя, которому приглашение.
        /// </summary>
        public string UserAvatarTo { get; set; }

        /// <summary>
        /// Просмотрено ли приглашение.
        /// </summary>        
        public bool IsViewed { get; set; }
    }
}