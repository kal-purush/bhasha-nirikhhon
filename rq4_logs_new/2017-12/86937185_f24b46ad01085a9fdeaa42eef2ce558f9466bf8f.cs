using System;
using System.Collections.Generic;
using System.Linq;
using System.Data;
using System.Data.SqlClient;
using QTFK.Models;
using QTFK.Extensions.DBCommand;
using QTFK.Extensions.DataReader;
using QTFK.Services.Loggers;

namespace QTFK.Services.DBIO
{
    public abstract class AbstractDBIO<TConnection, TCommand, TDataAdapter> : IDBIO
        where TConnection : IDbConnection
        where TCommand : IDbCommand
        where TDataAdapter : IDbDataAdapter, IDisposable
    {
        protected const string ERROR_MESSAGE_DEFAULT = "Error ocurred executing SQL instructions";
        protected const string ERROR_MESSAGE_GETTING_ID = "Error ocurred getting las ID";

        protected readonly string connectionString;
        protected readonly ILogger<LogLevel> log;

        public AbstractDBIO(string connectionString)
            : this(connectionString, NullLogger.Instance)
        {
        }

        public AbstractDBIO(
            string connectionString
            , ILogger<LogLevel> log
            )
        {
            Asserts.isSomething(this.log, "Argument 'log' cannot be null.");
            Asserts.isFilled(this.connectionString, "Argument 'connectionString' cannot be empty");

            this.connectionString = connectionString;
            this.log = log;
        }

        protected abstract TConnection prv_buildConnection(string connectionString);
        protected abstract TDataAdapter prv_buildDataAdapter(string query, TConnection connection);
        protected abstract TCommand prv_buildCommand(TConnection connection);

        public virtual void Dispose()
        {
        }

        public virtual DataSet Get(string query, IDictionary<string, object> parameters)
        {
            DataSet ds;

            Asserts.isFilled(query, "Argument 'query' cannot be empty.");
            Asserts.isSomething(parameters, "Argument 'parameters' cannot be null.");

            ds = null;

            using (TConnection conn = prv_buildConnection(this.connectionString))
            using (TDataAdapter da = prv_buildDataAdapter(query, conn))
            {
                try
                {
                    da.SelectCommand.AddParameters(parameters);
                    ds = new DataSet();
                    da.Fill(ds);
                }
                catch (Exception ex)
                {
                    throw prv_wrapException(query, ex, this.log, ERROR_MESSAGE_DEFAULT);
                }
                finally
                {
                    conn?.Close();
                }
            }

            Asserts.isSomething(ds, "Result DataSet is null!");
            return ds;
        }

        public virtual IEnumerable<T> Get<T>(string query, IDictionary<string, object> parameters, Func<IDataRecord, T> buildDelegate)
        {
            IEnumerable<T> result;
            IDbTransaction trans;
            IDataReader reader;

            Asserts.isFilled(query, "Argument 'query' cannot be empty.");
            Asserts.isSomething(parameters, "Argument 'parameters' cannot be null.");
            Asserts.isSomething(buildDelegate, "Argument 'buildDelegate' cannot be null.");

            result = null;

            using (TConnection conn = prv_buildConnection(this.connectionString))
            using (TCommand command = prv_buildCommand(conn))
            {
                trans = null;
                reader = null;

                try
                {
                    conn.Open();
                    trans = conn.BeginTransaction();

                    command.Transaction = trans;
                    command.CommandText = query;
                    command.AddParameters(parameters);

                    reader = command.ExecuteReader();
                    result = reader
                        .GetRecords()
                        .Select(buildDelegate)
                        .ToList()
                        ;
                }
                catch (Exception ex)
                {
                    throw prv_wrapException(command, ex, this.log, ERROR_MESSAGE_DEFAULT);
                }
                finally
                {
                    if (reader != null && !reader.IsClosed)
                        reader.Close();
                    trans?.Rollback();
                    conn?.Close();
                }
            }

            Asserts.isSomething(result, $"Output '{nameof(IEnumerable<T>)}' is null!");
            return result;
        }

        public virtual object GetLastID(IDbCommand cmd)
        {
            try
            {
                return cmd
                    .SetCommandText(" SELECT @@IDENTITY ")
                    .ClearParameters()
                    .ExecuteScalar()
                    ;
            }
            catch (Exception ex)
            {
                throw prv_wrapException(cmd, ex, this.log, ERROR_MESSAGE_GETTING_ID);
            }
        }

        public virtual int Set(Func<IDbCommand, int> instructions)
        {
            int affectedRows;
            IDbTransaction trans;

            Asserts.isSomething(instructions, "Argument 'instructions' cannot be null.");

            affectedRows = 0;

            using (TConnection conn = prv_buildConnection(this.connectionString))
            using (TCommand command = prv_buildCommand(conn))
            {
                trans = null;

                try
                {
                    conn.Open();
                    trans = conn.BeginTransaction();
                    command.Transaction = trans;
                    affectedRows = instructions(command);
                    trans.Commit();
                }
                catch (Exception ex)
                {
                    trans?.Rollback();
                    throw prv_wrapException(command, ex, this.log, ERROR_MESSAGE_DEFAULT);
                }
                finally
                {
                    conn?.Close();
                }
            }
            return affectedRows;
        }

        protected static DBIOException prv_wrapException(IDbCommand cmd, Exception ex, ILogger<LogLevel> logger, string subject)
        {
            return prv_wrapException(cmd?.CommandText ?? "", ex, logger, subject);
        }

        protected static DBIOException prv_wrapException(string query, Exception ex, ILogger<LogLevel> logger, string subject)
        {
            string message = $@"{subject}:
Exception: {ex.GetType().Name}
Message: {ex.Message}
Command: '{query}'";

            logger.log(LogLevel.Error, message);

            return new DBIOException(message, ex);
        }
    }
}