using System;
using System.Collections.Generic;
using System.Text;

namespace AutoVersionsDB.Core
{
    public static class CoreTextResources
    {
        public const string ProjectConfigValidation = "Project Config Validation Error";
        public const string ProjectIdNotExistValidation = "Validate Project Id Not Exist";

        public const string MandatoryFieldErrorMessage = "'[FieldName]' is mandatory";



        public const string AdminConnectionStringValidatorErrorMessage = "Could not connect to the Database with the following connection String: '[ConnectionString]'. Error Message: '[exMessage]'";
        public const string ConnectionStringValidatorErrorMessage = "Could not connect to the Database with the following properties: '[DBConnectionInfo]', Check each of the above properties. The used connection String was: '[ConnectionString]'. Error Message: '[exMessage]'";
        public const string DBTypeCodeNotValidErrorMessage = "The DBType: '[DBTypeCode]' is not valid";
        public const string DeliveryArtifactFolderNotExistErrorMessage = "Delivery Artifact Folder is not exist";
        public const string DevScriptsFolderNotExistErrorMessage = "Scripts Folder is not exist";
        public const string ScriptTypeFolderNotExistErrorMessage = "'[ScriptFolderPath]' Scripts Folder is not exist";
        public const string ProjectIdIsNotExistErrorMessage = "Project Id: '[Id]' is not exist.";
        public const string ProjectIdIsAlreadyExistErrorMessage = "Project Id is already exist";
        public const string ProjectIdIsAlreadyExistWithIdErrorMessage = "Project Id: '[Id]' is already exist";
        public const string ErrorMessage = "";

    }
}