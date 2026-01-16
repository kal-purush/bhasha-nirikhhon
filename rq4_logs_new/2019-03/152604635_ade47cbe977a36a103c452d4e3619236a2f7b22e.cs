using System;
using System.IO;

namespace ReleaseTool
{
    internal static class CommandsCommon
    {
        private const string ReleaseBranchNameTemplate = "feature/release-{0}";

        public const string GdkPinnedFilename = "gdk.pinned";

        public static void UpdateGdkVersion(GitClient gitClient, string gdkVersion)
        {
            if (!File.Exists(GdkPinnedFilename))
            {
                throw new InvalidOperationException("Could not upgrade gdk version as the file, " +
                                                    $"{GdkPinnedFilename}, does not exist");
            }
            
            File.WriteAllText(GdkPinnedFilename, gdkVersion);

            gitClient.StageFile(GdkPinnedFilename, UriKind.Relative);
        }

        public static string GetReleaseBranchName(string version)
        {
            return string.Format(ReleaseBranchNameTemplate, version);
        }
    }
}