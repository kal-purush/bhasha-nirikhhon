using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;

/// <summary>
/// Summary description for LogHelper
/// </summary>
public class LogHelper
{
    public enum ErrorType
    {
        Error = 0,
        Info = 1
    }

    public LogHelper()
    {
        //
        // TODO: Add constructor logic here
        //
    }

    /// <summary>
    /// Write log to txt file
    /// </summary>
    /// <param name="filePath">Source file path</param>
    /// <param name="errorType">Errory type</param>
    /// <param name="ex">Exception</param>
    public static void Log(string filePath, ErrorType errorType, Exception ex)
    {
        string log = DateTime.UtcNow.ToLocalTime() + " - " + errorType.ToString() + " - Filepath = " + filePath + " - StackTrack = " + ex.StackTrace + " - Message = " + ex.Message;
        // WriteAllText creates a file, writes the specified string to the file,
        // and then closes the file.    You do NOT need to call Flush() or Close().
        if (System.IO.File.Exists(HttpContext.Current.Server.MapPath("~/log.txt")))
        {
            using (System.IO.StreamWriter file = new System.IO.StreamWriter(HttpContext.Current.Server.MapPath("~/log.txt")))
            {
                file.WriteLine(log);
            }
        }
        else
        {
            System.IO.File.WriteAllText(HttpContext.Current.Server.MapPath("~/log.txt"), log);
        }
    }
}