using System;
using System.Threading;
using System.Web.Script.Services;
using System.Web.Services;
using System.Web.UI;

public partial class _Default : Page
{
	[WebMethod]
	[ScriptMethod(ResponseFormat = ResponseFormat.Json)]
	public static DateTime GetServerTime(string param1)
	{
        Thread.Sleep(10000);
		return DateTime.Now;
	}
}