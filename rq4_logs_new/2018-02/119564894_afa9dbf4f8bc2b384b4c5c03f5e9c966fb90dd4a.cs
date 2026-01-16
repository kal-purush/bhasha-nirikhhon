using DockerCliWrapper.Docker.Constants;
using DockerCliWrapper.Extensions;
using DockerCliWrapper.Infrastructure;
using System.Threading.Tasks;

namespace DockerCliWrapper.Docker.Container
{
    /// <summary>
    /// Management (start/stop etc) operations for local Docker containers. <see cref="https://docs.docker.com/engine/reference/commandline/container_start/"/>
    /// </summary>
    public class DockerContainer
    {
        private const string DefaultArg = " container";

        private const string StopArg = "stop";
        private const string StartArg = "start";
        private const string RestartArg = "restart";
        private const string PauseArg = "pause";
        private const string KillArg = "kill";
        private const string InspectArg = "inspect";

        private readonly IShellExecutor _shellExecutor;

        public string ContainerId { get; }

        public DockerContainer(string containerId)
            : this(containerId, ShellExecutor.Instance)
        {
            ContainerId = containerId;
        }

        public DockerContainer(string containerId, IShellExecutor shellExecutor)
        {
            ContainerId = containerId;

            _shellExecutor = shellExecutor;
        }

        /// <summary>
        /// Stop the container.
        /// </summary>
        /// <returns>True if the container was stopped successfully, otherwise false along with the error message.</returns>
        public async Task<Result> Stop()
        {
            return await Stop(-1);
        }

        /// <summary>
        /// Stop the container and wait for the given number of seconds before killing the process
        /// when unresponsive.
        /// </summary>
        /// <param name="secondsToWaitBeforeKilling">Number of seconds to wait before killing the process (default is 10 seconds).</param>
        /// <returns>True if the container was stopped successfully, otherwise false along with the error message.</returns>
        public async Task<Result> Stop(int secondsToWaitBeforeKilling)
        {
            string options = secondsToWaitBeforeKilling == -1 ? " " : $" -t {secondsToWaitBeforeKilling} ";

            return await _shellExecutor.ExecuteWithResult($"{DefaultArg} {StopArg}{options}{ContainerId}");
        }

        /// <summary>
        /// Start the container.
        /// </summary>
        /// <returns>True if the container was started successfully, otherwise false along with the error message.</returns>
        public async Task<Result> Start()
        {
            return await _shellExecutor.ExecuteWithResult($"{DefaultArg} {StartArg} {ContainerId}");
        }

        /// <summary>
        /// Restart the container.
        /// </summary>
        /// <returns>True if the container was restarted successfully, otherwise false along with the error message.</returns>
        public async Task<Result> Restart()
        {
            return await Restart(-1);
        }

        /// <summary>
        /// Restart the container and wait for the given number of seconds before killing the process
        /// when unresponsive.
        /// </summary>
        /// <param name="secondsToWaitBeforeKilling">Number of seconds to wait before killing the process (default is 10 seconds).</param>
        /// <returns>True if the container was restarted successfully, otherwise false along with the error message.</returns>
        public async Task<Result> Restart(int secondsToWaitBeforeKilling)
        {
            string options = secondsToWaitBeforeKilling == -1 ? " " : $" -t {secondsToWaitBeforeKilling} ";

            return await _shellExecutor.ExecuteWithResult($"{DefaultArg} {RestartArg}{options}{ContainerId}");
        }

        /// <summary>
        /// Pause all processes within the container.
        /// </summary>
        /// <returns>True if the container process was paused successfully, otherwise false along with the error message.</returns>
        public async Task<Result> Pause()
        {
            return await _shellExecutor.ExecuteWithResult($"{DefaultArg} {PauseArg} {ContainerId}");
        }

        /// <summary>
        /// Kill the container.
        /// </summary>
        /// <returns>True if the container was killed successfully, otherwise false along with the error message.</returns>
        public async Task<Result> Kill()
        {
            return await _shellExecutor.ExecuteWithResult($"{DefaultArg} {KillArg} {ContainerId}");
        }

        /// <summary>
        /// Display detailed information of the container.
        /// </summary>
        /// <returns>True if the container details were returned successfully, otherwise false along with the error message.</returns>
        public async Task<Result> Inspect()
        {
            return await _shellExecutor.ExecuteWithResult($"{DefaultArg} {InspectArg} {ContainerId}");
        }
    }
}