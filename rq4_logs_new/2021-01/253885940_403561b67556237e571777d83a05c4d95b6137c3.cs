using AutoVersionsDB.Core.DBVersions.ScriptFiles;
using AutoVersionsDB.DB;
using AutoVersionsDB.DB.Contract;
using AutoVersionsDB.NotificationableEngine;
using AutoVersionsDB.NotificationableEngine.Validations;
using System.Collections.Generic;
using System.Linq;

namespace AutoVersionsDB.Core.DBVersions.Processes.Validators
{
    public class NewProjectValidator : ValidatorBase
    {
        private readonly DBCommandsFactory _dbCommandsFactory;
        private readonly DBConnectionInfo _dbConnectionInfo;
        private readonly bool _isDevEnvironment;
        private readonly ScriptFilesState _scriptFilesState;

        public override string ValidatorName => "NewProjectValidator";

        private string _errorInstructionsMessage;
        public override string ErrorInstructionsMessage => _errorInstructionsMessage;

        public override NotificationErrorType NotificationErrorType => NotificationErrorType.Attention;


        public NewProjectValidator(DBCommandsFactory dbCommandsFactory,
                                        bool isDevEnvironment,
                                        DBConnectionInfo dbConnectionInfo,
                                        ScriptFilesState scriptFilesState)
        {
            _dbCommandsFactory = dbCommandsFactory;
            _isDevEnvironment = isDevEnvironment;
            _dbConnectionInfo = dbConnectionInfo;
            _scriptFilesState = scriptFilesState;
        }


        public override string Validate()
        {
            using (var dbCommands = _dbCommandsFactory.CreateDBCommand(_dbConnectionInfo))
            {
                if (!dbCommands.CheckIfTableExist(DBCommandsConsts.DBSchemaName, DBCommandsConsts.DBScriptsExecutionHistoryTableName)
                    && !dbCommands.CheckIfTableExist(DBCommandsConsts.DBSchemaName, DBCommandsConsts.DBScriptsExecutionHistoryFilesTableName))
                {
                    

                    if (_isDevEnvironment)
                    {
                        if (_scriptFilesState.IncrementalScriptFilesComparer.AllFileSystemScriptFiles.Count == 0
                            && _scriptFilesState.RepeatableScriptFilesComparer.AllFileSystemScriptFiles.Count == 0
                            && _scriptFilesState.DevDummyDataScriptFilesComparer.AllFileSystemScriptFiles.Count == 0)
                        {
                            _errorInstructionsMessage = "Welcome to AutoVersionsDB :-) 1. Run 'Recreate' or 'Virtual' for creating our DB system tables; 2. Add your scripts; 3. Run 'Sync';";
                            string errorMsg = $"No scripts file, and no system tables on the DB, its probably a new project on dev environment.";
                            return errorMsg;
                        }
                    }
                    else
                    {
                        if (_scriptFilesState.IncrementalScriptFilesComparer.AllFileSystemScriptFiles.Count == 0
                            && _scriptFilesState.RepeatableScriptFilesComparer.AllFileSystemScriptFiles.Count == 0)
                        {
                            _errorInstructionsMessage = "Welcome to AutoVersionsDB :-) 1. Copy the artifact file that deployed from your dev environment; 2. Run 'Virtual' to set the current DB state related to the scripts file; 3. Run 'Sync' for executing the rest of the scripts files;";
                            string errorMsg = $"No scripts file, and no system tables on the DB, its probably a new project on delivery environment.";
                            return errorMsg;
                        }
                        else
                        {
                            _errorInstructionsMessage = "Welcome to AutoVersionsDB :-) 1. Run 'Virtual' to set the current DB state related to the scripts file; 2. Run 'Sync' for executing all other scripts files on the DB;";
                            string errorMsg = $"No system tables on the DB, its probably a new project on delivery environment.";
                            return errorMsg;
                        }
                    }

                }

            }


            return "";

        }
    }
}