using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace MusicPlayerBackend.InternalTypes
{
    public class EnumerateFilesAbortedException : Exception
    {
        public EnumerateFilesAbortedException()
        {
        }

        public EnumerateFilesAbortedException(string message)
            : base(message)
        {
        }

        public EnumerateFilesAbortedException(string message, Exception inner)
            : base(message, inner)
        {
        }
    }
}