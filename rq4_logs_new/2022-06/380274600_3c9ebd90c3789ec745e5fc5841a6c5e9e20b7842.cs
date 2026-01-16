using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Orangebeard.SpecFlowPlugin.ClientExecution.Logging
{
    public class LogMessageAttachment
    {
        public LogMessageAttachment(string mimeType, byte[] data)
        {
            MimeType = mimeType;
            Data = data; //TODO?~ Explicit array copy? Safer but takes time.
        }

        public string MimeType { get; private set; }
        public byte[] Data { get; private set; }
    }
}