using System;
using System.Collections.Generic;
using System.Text;

namespace AutoVersionsDB.CLI
{
    public static class CLITextResources
    {
        //CLI Process Messages
        public const string StartProcessMessageNoArgs = "> Run '[processName]' (no arguments)";
        public const string StartProcessMessageWithArgs = "> Run '[processName]' for '[args]'";
        public const string ProcessCompleteWithErrors = "The process complete with errors:";
        public const string ProcessCompleteSuccessfully = "The process complete successfully";

        //CLI ConfigProjects Options Description
        public const string IdCLIOptionDescription = "The project Id";
        public const string BackupFolderPathCLIOptionDescription = "Backup up folder path for saving the database before run";
        public const string DBNameCLIOptionDescription = "Data Base Name";
        public const string DBTypeCLIOptionDescription = "";
        public const string Description = "Database Type (SqlServer)";
        public const string DeliveryArtifactFolderPathCLIOptionDescription = "For delivery environment only - The folder where artifact file is located";
        public const string DeployArtifactFolderPathCLIOptionDescription = "Deploy folder path for exporting the artifact file";
        public const string DescriptionCLIOptionDescription = "Description for the project";
        public const string DevEnvironmentCLIOptionDescription = "Is the project run on dev environment (use scripts files) or on a delivery environment (use artifact file)";
        public const string NewIdCLIOptionDescription = "The new project Id";
        public const string PasswordCLIOptionDescription = "DB Password";
        public const string ScriptsBaseFolderPathCLIOptionDescription = "For dev environment only - the scripts base folder path. Where all the project scripts files located. Usually, it is a folder the the source control can follow.";
        public const string ServerCLIOptionDescription = "DB Server Instance Name";
        public const string UsernameCLIOptionDescription = "DB Username";

        //CLI ConfigProjects Commands Description
        public const string ChangeIdCommandDescription = "Change project project id. Change the identifier of the project.";
        public const string ConfigCommandDescription = "Change project configuration.";
        public const string DBTypesCommandDescription = "Show list of the supported DataBase Types.";
        public const string EnvironmentCommandDescription = "Change project environment configuration. Is run on dev environment (use scripts files) or on a delivery environment (use artifact file).";
        public const string InitCommandDescription = "Initiate project. Define a new project for AutoVersionsDB.";
        public const string ListCommandDescription = "Show list of all projects";
        public const string RemoveCommandDescription = "Remove project";

        //CLI ConfigProjects Commands Error
        public const string IdNotExistCommandError = "Id: '[Id]' is not exist. You can use the 'init' command to define new project or the 'list' command to see the existing projects on this machine";

        //CLI ConfigProjects Commands Info
        public const string ProjectNotFoundInfoMessage = "----  No projects found on this machine  ----";


        //CLI DBVersions Options Description
        public const string TargetCLIOptionDescription = "The [ScriptFileType] target file script name that set the db in the desired state. Set '[TargetNoneScriptFileName]' if you want to that system not run any [ScriptFileType] script; Set '[TargetLastScriptFileName]' if you want to that system not run all [ScriptFileType] scripts; Otherwise, set the target script name.";
        public const string ScriptNameCLIOptionDescription = "New script name";

        //CLI DBVersions Commands Description
        public const string CreateNewDevDummyDataScriptFileCommandDescription = "Create new dev dummy data script file";
        public const string CreateNewIncrementalScriptFileCommandDescription = "Create new incremental script file";
        public const string CreateNewRepeatableScriptFileCommandDescription = "Create new repeatable script file";
        public const string CreateNewScriptFileCommandDescription = "Create new script file. The type of script file is required.";
        public const string DeployCommandDescription = "Deploy the project. Create an artifact file ready to use on delivery enviornment.";
        public const string FilesCommandDescription = "Show list of the scripts files.";
        public const string FilesSignleTypeCommandDescription = "Show only the [ScriptFileType] scripts files.";
        public const string RecreateCommandDescription = "Recreate the database from scratch to the last state only by the scripts files";
        public const string SyncCommandDescription = "Sync the database to the last state by the scripts files";
        public const string ValidateCommandDescription = "Validate all relevant relevant properties for run sync on the project.";
        public const string VirtualCommandDescription = "Set the Database to specific state by virtually executions the scripts file. This command is useful when production database didnt use this tool yet. Insert into the 'Targets' options (-it, -rt, -dt) the target script file name that you want to set the db state.";

        //CLI DBVersions Commands Info
        public const string TheFileIsCreatedInfoMessage = "The file: '[newFilePath]' is created.";
        public const string ArtifactFileCreatedInfoMessage = "Artifact file created: '[deployFilePath]'";

    }
}