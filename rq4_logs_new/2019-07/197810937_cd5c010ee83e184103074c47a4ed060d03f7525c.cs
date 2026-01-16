using System;
using UnityEngine;

public class Asserts
{
	public static void Fatal(bool condition, string fmt)
	{
		if (!condition)
		{
			Fail(fmt);
		}
	}

	public static void Fatal(bool condition, string fmt, object arg1)
	{
		if (!condition)
		{
			Fail(fmt, arg1);
		}
	}

	public static void Fatal(bool condition, string fmt, object arg1, object arg2)
	{
		if (!condition)
		{
			Fail(fmt, arg1, arg2);
		}
	}

	public static void Fatal(bool condition, string fmt, object arg1, object arg2, object arg3)
	{
		if (!condition)
		{
			Fail(fmt, arg1, arg2, arg3);
		}
	}

	private static void Fail(string fmt, params object[] args)
	{
		try
		{
			if (System.Diagnostics.Debugger.IsAttached)
				System.Diagnostics.Debugger.Break();

			Debug.LogErrorFormat(fmt, args);
		}
		catch (FormatException)
		{
			Debug.LogError(fmt + string.Join(", ", args));
		}
#if !UNITY_EDITOR
            Application.Quit(1);
#endif
	}
}