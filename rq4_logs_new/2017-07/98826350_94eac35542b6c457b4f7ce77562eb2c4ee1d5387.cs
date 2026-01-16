using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DTOperator
{
	class Const
	{
		public static readonly String VERSION = "4.1:{git revision}_win";

		public static readonly String USERSFILE = "C:\\Users\\A\\dtoperator\\users.conf";
		public static readonly String CONFFILE = "C:\\Users\\A\\dtoperator\\dtoperator.conf";
		public static readonly int COMMANDSIZE = 2048;
		public static readonly int MEDIASIZE = 1200;
		public static readonly int MAXLISTENWAIT = 5;
		public static readonly int MARGIN_OF_ERROR = 5;
		public static readonly int CHALLENGE_LENGTH = 200;
		public static readonly int SESSIONKEY_LENGTH = 59;

		public static readonly String ENCAES_PLACEHOLDER = "ENCRYPTED_AES_HERE";
		public static readonly String SESSIONKEY_PLACEHOLDER = "SESSION_KEY_HERE";

		public static readonly String JBYTE = "D";

		public static readonly int UNAUTHTIMEOUT = 500;
		public static readonly int AUTHTIMEOUT = 2000;

		public static readonly String LOGFOLDER = "C:\\Users\\A\\dtoperator\\log\\";

		public static readonly int DEFAULTCMD = 1991;
		public static readonly int DEFAULTMEDIA = 1961;
		public static readonly int IPTOS_DSCP_EF = 0xB8;

		public enum ustate { NONE, INIT, INCALL, INVALID };
	}
}