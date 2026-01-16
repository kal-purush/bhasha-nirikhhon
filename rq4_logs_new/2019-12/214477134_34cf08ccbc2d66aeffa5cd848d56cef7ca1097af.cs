using System;
using System.Collections.Generic;
using System.Text;

namespace CentralDeErros.CrossCutting.CustomTypes
{
    public static class ConfigurationConst
    {
        // This configurations can be set at appsettings or environment variables
        public const string DefaultDatabase = "DefaultDatabase";
        public const string DefaultConnection = "DefaultConnection";
        public const string DeployEnvironment = "DEPLOY_ENVIRONMENT";
        public const string ConnectionString = "CONNECTION_STRING";
        public const string DatabaseURL = "DATABASE_URL"; 
    }

    public static class DeployEnvironment
    {
        public const string Heroku = "HEROKU";
    }
}