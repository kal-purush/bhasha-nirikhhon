using System;
using System.Collections.Generic;
using System.Text;

namespace AutoVersionsDB.UI
{
    public static class UITextResources
    {
        //Projects
        public const string DeleteProjectConfirmaion = "Are you sure you want to delete the configurration for the project: '[Id]'";


        //DBVersions
        public const string BtnRefreshTooltip = "Refresh";
        public const string BtnRunSyncTooltip = "Sync the db with the missing scripts";
        public const string BtnRecreateDBFromScratchMainTooltip = "Recreate DB From Scratch";
        public const string BtnDeployTooltip = "Create Deploy Package";
        public const string BtnSetDBToSpecificStateTooltip = "Set DB To Specific State";
        public const string BtnVirtualExecutionTooltip = "Set DB to specific state virtually. Use it if your DB is not empty but you never use our migration tool on it yet.";
        public const string BtnShowHistoricalBackupsTooltip = "Open the backup history folder.";
   
        public const string CreateNewScriptFileInstructions = "Create new script script file, insert the script name:";
        public const string RecreateDBConfirmaion = "This action will drop the Database and recreate it only by the scripts, you may loose Data. Are you sure?";
        public const string TargetStateHistoryConfirmaion = "This action will drop the Database and recreate it only by the scripts, you may lose Data. Are you sure?";

        public const string SyncToSpecificStateInstructions = "Select the target Database State (on every script type), and click on Apply";
        public const string SetVirtualInstructions = "Select the Target (on every script type) Database State to virtually mark, and click on Apply";



        //Notification
        public const string WaitingForUserMessage = "Waiting for your command.";
        public const string PreparingMessage = "Please wait, preparing...";
        public const string CompleteSuccessfullyMessage = "The process complete successfully";
    }
}