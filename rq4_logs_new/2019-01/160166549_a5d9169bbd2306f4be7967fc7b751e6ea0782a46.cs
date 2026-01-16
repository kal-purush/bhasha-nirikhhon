using System;

namespace GtdCommon.ModelsDto
{
    /// <summary>
    /// class for TaskRecord model
    /// </summary>
    public class TaskRecordDto
    {
        /// <summary>
        /// Gets or sets a value of Id property
        /// </summary>
        public int Id { get; set; }

        /// <summary>
        /// Gets or sets a value of Name property
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Gets or sets a value of Description value
        /// </summary>
        public string Description { get; set; }

        /// <summary>
        /// Gets or sets a value of Action value
        /// </summary>
        public string Action { get; set; }

        /// <summary>
        /// Gets or sets a value of StartTime value
        /// </summary>
        public DateTime StartTime { get; set; }

        /// <summary>
        /// Gets or sets a value of StopTime value
        /// </summary>
        public DateTime StopTime { get; set; }

        /// <summary>
        /// Gets or sets a value of ElapsedTime value
        /// </summary>
        public int ElapsedTime { get; set; }

        /// <summary>
        /// Gets or sets a value of TaskId value
        /// </summary>
        public int TaskId { get; set; }
    }
}