using Domain.Common;
using Persistence.Database;

namespace API.App_Exceptions
{
    public interface IExceptionService
    {
        void LogException(Exception exception);
    }
    public class ExceptionService : IExceptionService
    {
        private readonly DataContext _context;
        public ExceptionService(DataContext context)
        {
            _context = context;
        }
        public void LogException(Exception exception)
        {
            ExceptionLog exceptionLog = new ExceptionLog();
            exceptionLog.Message = exception.Message;
            exceptionLog.StackTrace = exception.StackTrace;
            exceptionLog.Source = exception.Source;
            exceptionLog.CreatedDate = DateTime.UtcNow;
            _context.ExceptionLogs.Add(exceptionLog);
            _context.SaveChanges();
        }
    }
}