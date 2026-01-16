using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Pc.Information.Model.User
{
    /// <summary>
    /// User email activation model
    /// </summary>
    public class UserEmailActivationModel
    {
        /// <summary>
        /// user id
        /// </summary>
        public int UserId { get; set; }

        /// <summary>
        /// End time
        /// </summary>
        public DateTime EndTime { get; set; }

        /// <summary>
        /// Guid
        /// </summary>
        public string GuId { get; set; }
    }
}