using System;

namespace DatabaseCommand
{
    public abstract class MyDbConnection
    {
        public string ConnectionString { get; }
        
        public TimeSpan TimeOut { get; }

        public MyDbConnection(string connectionString)
        {
            if (connectionString == null)
                throw new ArgumentNullException(connectionString);
            
            if (connectionString == "")
                throw new ArgumentException("Argument must not be an empty string", connectionString);
            
            ConnectionString = connectionString;
        }

        public abstract void OpenConnection();

        public abstract void CloseConnection();

    }
}