using System;
using System.Collections.Generic;
using System.Text;

namespace LogCollections
{
    /// <summary>
    /// How the compactification operation is triggered.
    /// </summary>
    public enum CompactificationMode
    {
        /// <summary>
        /// Trigger compactification once a specific number of files have been written.
        /// </summary>
        FileCount = 0,
        /// <summary>
        /// Trigger compactification once a specific count of append operations is reached.
        /// </summary>
        OperationCount = 2,
    }
}