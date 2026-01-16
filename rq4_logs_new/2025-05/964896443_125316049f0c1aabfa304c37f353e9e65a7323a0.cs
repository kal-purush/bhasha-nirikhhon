using System.Collections.Generic;

namespace Smartitecture.ViewModels
{
    /// <summary>
    /// Represents an action performed by the agent.
    /// </summary>
    public class AgentAction
    {
        /// <summary>
        /// Gets or sets the name of the tool used.
        /// </summary>
        public string ToolName { get; set; }

        /// <summary>
        /// Gets or sets the input provided to the tool.
        /// </summary>
        public Dictionary<string, object> Input { get; set; }

        /// <summary>
        /// Gets or sets the output received from the tool.
        /// </summary>
        public object Output { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether the tool execution was successful.
        /// </summary>
        public bool IsSuccessful { get; set; }
    }
}