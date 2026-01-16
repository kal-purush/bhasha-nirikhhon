using JetBrains.Annotations;
using Nexus.Link.Capabilities.WorkflowConfiguration.Abstract.Entities;
using Nexus.Link.WorkflowEngine.Sdk.Internal.Logic;
using Nexus.Link.WorkflowEngine.Sdk.Support;

namespace Nexus.Link.WorkflowEngine.Sdk.Internal.Support;

internal class ActivityInformation : IActivityInformation
{
    private readonly ActivityDefinition _activityDefinition;

    private readonly MethodHandler _methodHandler;

    public ActivityInformation(IWorkflowInformation workflowInformation, int position, string formId)
    {
        Workflow = workflowInformation;
        Position = position;
        FormId = formId;
        Parent = Workflow.GetCurrentParentActivity();
        Previous = Workflow.LatestActivity;
        _activityDefinition = Workflow.GetActivityDefinition(formId);
        Options.From(Workflow.DefaultActivityOptions);
        _methodHandler = new MethodHandler(FormTitle);
    }

    /// <inheritdoc />
    public Activity Previous { get; }

    /// <inheritdoc />

    public string FormTitle => _activityDefinition.Title;

    public ActivityTypeEnum Type => _activityDefinition.Type;

    /// <inheritdoc />
    public ActivityOptions Options { get; } = new ();

    /// <inheritdoc />
    public void DefineParameter<T>(string name)
    {
        _methodHandler.DefineParameter<T>(name);
    }

    /// <inheritdoc />
    public void SetParameter<T>(string name, T value)
    {
        _methodHandler.SetParameter(name, value);
    }

    /// <inheritdoc />
    public T GetArgument<T>(string parameterName)
    {
        return _methodHandler.GetArgument<T>(parameterName);
    }

    /// <inheritdoc />
    public IWorkflowInformation Workflow { get; }

    /// <inheritdoc />
    public int Position { get; }

    /// <inheritdoc />
    public string FormId { get; }

    /// <inheritdoc />
    public Activity Parent { get; }
}

internal interface IActivityInformation
{
    /// <summary>
    /// All available information about a workflow
    /// </summary>
    IWorkflowInformation Workflow { get; }

    /// <summary>
    /// If the current activity is a child activity, then this is the parent activity for this child, otherwise null.
    /// </summary>
    [CanBeNull]
    Activity Parent { get; }

    /// <summary>
    /// The previous activity
    /// </summary>
    [CanBeNull] Activity Previous { get; }

    /// <summary>
    /// The relative position in the workflow for this activity.
    /// </summary>
    int Position { get; }

    /// <summary>
    /// The identity of the activity form
    /// </summary>
    string FormId { get; }

    /// <summary>
    /// The title for this activity form
    /// </summary>
    public string FormTitle { get; }

    /// <summary>
    /// The type for this activity form
    /// </summary>
    public ActivityTypeEnum Type{ get; }

    /// <summary>
    /// The options for this activity
    /// </summary>
    ActivityOptions Options { get; }

    /// <summary>
    /// Define an activity parameter named <paramref name="name"/> with type <typeparamref name="T"/>.
    /// </summary>
    void DefineParameter<T>(string name);

    /// <summary>
    /// Set the parameter named <paramref name="name"/> to the value <paramref name="value"/>.
    /// </summary>
    void SetParameter<T>(string name, T value);

    /// <summary>
    /// Get the value of parameter <paramref name="parameterName"/> with type <typeparamref name="T"/>.
    /// </summary>
    T GetArgument<T>(string parameterName);
}