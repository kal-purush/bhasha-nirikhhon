//-----------------------------------------------------------------------
// <copyright file="Alarm.cs" company="SoftServe">
//     Company copyright tag.
// </copyright>
//-----------------------------------------------------------------------

namespace GtdTimerDAL.Entities
{
    public class Alarm
    {        /// <summary>
             /// Gets or sets Id column
             /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets CronExpression column
        /// </summary>
        public string CronExpression { get; set; }

        /// <summary>
        /// Gets or sets IsTurnOn column
        /// </summary>
        public bool IsTurnOn { get; set; }

        /// <summary>
        /// Gets or sets IsSound column
        /// </summary>
        public bool IsSound { get; set; }

        /// <summary>
        /// Gets or sets Message column
        /// </summary>
        public string Message { get; set; }

        /// <summary>
        /// Gets or sets UserId column
        /// </summary>
        public int UserId { get; set; }

        /// <summary>
        /// Gets or sets Foreign key to user table
        /// </summary>
        public User User { get; set; }
    }
}