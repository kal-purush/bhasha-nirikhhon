using DockerCliWrapper.Infrastructure;

namespace DockerCliWrapper.Docker.Image
{
    /// <summary>
    /// Management (create/delete etc) operations for local images. <see cref="https://docs.docker.com/engine/reference/commandline/image/"/> 
    /// for full CLI details.
    /// </summary>
    public class DockerImage
    {
        private const string Command = "docker";
        private const string DefaultArg = "image";

        private const string RemoveFlag = "rm";

        /// <summary>
        /// Remove the image with the given name. If a container based on the image exists
        /// then it will not be removed.
        /// </summary>
        /// <param name="imageName">The image name to remove.</param>
        /// <param name="errorMessage">The error message if one occurs.</param>
        /// <returns>True if the image was deleted successfully, otherwise false.</returns>
        public bool Remove(string imageName, out string errorMessage)
        {
            return Remove(imageName, false, out errorMessage);
        }

        /// <summary>
        /// Forcibly remove the image with the given name, even when a container based on the image exists,
        /// in which case the container will also be deleted.
        /// </summary>
        /// <param name="imageName">The image name to remove.</param>
        /// <param name="errorMessage">The error message if one occurs.</param>
        /// <returns>True if the image was deleted successfully, otherwise false.</returns>
        public bool ForceRemove(string imageName, out string errorMessage)
        {
            return Remove(imageName, true, out errorMessage);
        }

        private bool Remove(string imageName, bool force, out string errorMessage)
        {
            Guard.IsNotNullOrEmpty(nameof(imageName), imageName);

            var result = ShellExecutor.Instance.Execute(Command, $" {DefaultArg} {RemoveFlag} {imageName}{(force ? " -f" : "" )}");

            if (result.IsSuccessFull)
            {
                errorMessage = "";
                return true;
            }

            errorMessage = result.Error;

            return false;
        }
    }
}