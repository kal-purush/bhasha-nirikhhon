using System.Diagnostics.CodeAnalysis;
using UnityEngine;

namespace Extreal.Core.Logging
{
    /// <summary>
    /// Class that holds the format of the log output by UnityDebugLogWriter.
    /// </summary>
    public class UnityDebugLogFormat
    {
        /// <summary>
        /// Target log category.
        /// </summary>
        public string Category { get; }

        /// <summary>
        /// Color for log.
        /// </summary>
        public string ColorRGB { get; }

        /// <summary>
        /// Creates UnityDebugLogFormat.
        /// </summary>
        /// <param name="category">Target log category.</param>
        /// <param name="color">Color for log.</param>
        [SuppressMessage("Usage", "CC0057")]
        public UnityDebugLogFormat(string category, Color color = default)
        {
            Category = category;
            ColorRGB = $"#{ColorUtility.ToHtmlStringRGB(color)}";
        }
    }
}