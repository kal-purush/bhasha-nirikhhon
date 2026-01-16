using CentralDeErros.Application.ViewModel;
using CentralDeErros.Domain.Models;
using System;

namespace CentralDeErros.Application.Extensions
{
    public static class FilterErrorLogExtension
    {
        public static Func<ErrorLog, bool> FilterErrorLog(this Func<ErrorLog, bool> source, ErrorLogFilter filter)
        {
            Func<ErrorLog, bool> predicateCode = p => true;
            Func<ErrorLog, bool> predicateMessage = p => true;
            Func<ErrorLog, bool> predicateLevel = p => true;
            Func<ErrorLog, bool> predicateArchieved = p => true;
            Func<ErrorLog, bool> predicateEnvironment = p => true;

            if (filter.Code != null)
                predicateCode = p => p.Code == filter.Code;
            if (filter.Message != null)
                predicateMessage = p => p.Message == filter.Message;
            if (filter.Level != null)
                predicateLevel = p => p.Level == filter.Level;
            if (filter.Archieved != null)
                predicateArchieved = p => p.Archieved == filter.Archieved;
            if (filter.Environment != null)
                predicateEnvironment = p => p.Environment == filter.Environment;

            return p => predicateCode(p) &&
                             predicateMessage(p) &&
                             predicateLevel(p) &&
                             predicateArchieved(p) &&
                             predicateEnvironment(p);
        }
    }
}