using System.Threading;
using System.Threading.Tasks;
using Nexus.Link.Capabilities.WorkflowConfiguration.Abstract.Entities;

namespace Nexus.Link.WorkflowEngine.Sdk.Interfaces;

/// <summary>
/// An activity of type <see cref="ActivityTypeEnum.Lock"/>, i.e. an activity that is executed while a resource is locked.
/// </summary>
public interface IActivityLock: IActivity
{
    /// <summary>
    /// The identifier of the resource we should lock
    /// </summary>
    string ResourceIdentifier { get; }

    /// <summary>
    /// Raise a semaphore with a maximum number of concurrent executions, execute the <paramref name="methodAsync"/>, lower the semaphore.
    /// </summary>
    Task ExecuteAsync(ActivityMethodAsync<IActivityLock> methodAsync, CancellationToken cancellationToken = default);
}
/// <summary>
/// An activity of type <see cref="ActivityTypeEnum.Lock"/>.
/// </summary>
public interface IActivityLock<TActivityReturns> : IActivity
{
    /// <summary>
    /// The identifier of the resource we should lock
    /// </summary>
    string ResourceIdentifier { get; }

    /// <summary>
    /// Raise a semaphore with a maximum number of concurrent executions, execute the <paramref name="methodAsync"/>, lower the semaphore.
    /// </summary>
    Task<TActivityReturns> ExecuteAsync(ActivityMethodAsync<IActivityLock<TActivityReturns>, TActivityReturns> methodAsync, CancellationToken cancellationToken = default);
}