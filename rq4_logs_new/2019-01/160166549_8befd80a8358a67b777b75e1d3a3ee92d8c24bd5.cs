//-----------------------------------------------------------------------
// <copyright file="PresetTasks.cs" company="SoftServe">
//     Company copyright tag.
// </copyright>
//-----------------------------------------------------------------------

namespace GtdTimerDAL.Entities
{
    /// <summary>
    /// Fields of preset tasks table
    /// </summary>
    public class PresetTasks
    {
        /// <summary>
        /// Gets or sets id column
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets preset id column
        /// </summary>
        public int PresetId { get; set; }

        /// <summary>
        /// Gets or sets preset foreign key
        /// </summary>
        public Preset Preset { get; set; }

        /// <summary>
        /// Gets or sets task id column
        /// </summary>
        public int TaskId { get; set; }

        /// <summary>
        /// Gets or sets roles foreign key
        /// </summary>
        public Tasks Task { get; set; }
    }
}