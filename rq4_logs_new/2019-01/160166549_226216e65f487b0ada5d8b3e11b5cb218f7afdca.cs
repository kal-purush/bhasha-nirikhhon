//-----------------------------------------------------------------------
// <copyright file="Tasks.cs" company="SoftServe">
//     Company copyright tag.
// </copyright>
//-----------------------------------------------------------------------


using System;

namespace GtdTimerDAL.Entities
{
    /// <summary>
    /// Fields of Record table
    /// </summary>
    public class Record
    {
        /// <summary>
        /// Primary key for Record
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Time when timer was started
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Time when timer was stopped
        /// </summary>
        public DateTime StopTime { get; set; }

        /// <summary>
        /// Foreign key that reference to Task
        /// </summary>
        public int TaskId { get; set; }

        /// <summary>
        /// Task that are referenced by this record
        /// </summary>
        public Tasks Task { get; set; }
    }
}