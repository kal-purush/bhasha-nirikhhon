using Nexus.Link.Capabilities.WorkflowState.Abstract.Entities;

namespace Nexus.Link.WorkflowEngine.Sdk.Exceptions
{
    /// <summary>
    /// Use this exception if your activity failed.
    /// </summary>
#pragma warning disable CS0618
    public class ActivityFailedException : ActivityException
#pragma warning restore CS0618
    {

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="exceptionCategory">The failure category</param>
        /// <param name="technicalMessage">A message directed to a developer</param>
        /// <param name="friendlyMessage">A message directed to a business person</param>
        public ActivityFailedException(ActivityExceptionCategoryEnum exceptionCategory, string technicalMessage, string friendlyMessage) : base(exceptionCategory, technicalMessage, friendlyMessage)
        {
        }
    }
}