using System;
using System.Collections.Generic;
using System.Text;

namespace Sig.Profile.Application.Common
{
    public class Utility
    {
        // <summary>
        /// Contains definitions for Log messages ready for String.Format
        /// </summary>
        #region Log Messages
        public const string LOG_ENTRY = "Entering method {0}";
        public const string LOG_EXIT = "Exiting method {0}";
        public const string LOG_ERROR_DETAILS = "Error:{0}, ErrorCode{1}, ErrorDetail{2}";
        #endregion
    }
}