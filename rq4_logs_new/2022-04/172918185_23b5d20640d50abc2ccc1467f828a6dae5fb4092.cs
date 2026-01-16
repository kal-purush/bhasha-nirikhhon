using System;
using Nexus.Link.Capabilities.WorkflowConfiguration.Abstract.Entities;
using Nexus.Link.WorkflowEngine.Sdk.Internal.Interfaces;
using Nexus.Link.WorkflowEngine.Sdk.Support;

namespace Nexus.Link.WorkflowEngine.Sdk.Interfaces;

/// <summary>
/// Properties that can be extracted from <see cref="IActivityInformation"/>
/// </summary>
public interface IActivityBase
{
    /// <summary>
    /// The instance id of the activity
    /// </summary>
    string ActivityInstanceId { get; }

    /// <summary>
    /// The form id of the activity
    /// </summary>
    string ActivityFormId { get; }

    /// <summary>
    /// The date and time when the workflow started
    /// </summary>
    DateTimeOffset WorkflowStartedAt { get; }

    /// <summary>
    /// The instance id of the workflow
    /// </summary>
    string WorkflowInstanceId { get; }

    /// <summary>
    /// The <see cref="ActivityOptions"/> for this activity.
    /// </summary>
    ActivityOptions Options { get; }

    /// <summary>
    /// The fail urgency for this activity
    /// </summary>
    [Obsolete("Please use Options.FailUrgency. Compilation warning since 2021-11-19.")]
    ActivityFailUrgencyEnum FailUrgency { get; }
}