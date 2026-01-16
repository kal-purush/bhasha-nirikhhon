namespace HardySoft.GpsTracker.Services.Models
{
    /// <summary>
    /// All possible statuses for GPS tracking.
    /// </summary>
    public enum TrackingStatus
    {
        /// <summary>
        /// Unknown status.
        /// </summary>
        Unknown,

        /// <summary>
        /// Tracking is topped.
        /// </summary>
        Stopped,

        /// <summary>
        /// Tracking is started.
        /// </summary>
        Started,

        /// <summary>
        /// Tracking is paused.
        /// </summary>
        Paused
    }
}