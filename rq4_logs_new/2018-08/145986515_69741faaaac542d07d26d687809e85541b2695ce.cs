using System;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using RGiesecke.DllExport;

namespace Economic
{
	/// <summary>
	/// Entry point
	/// </summary>
	public class DllEntry
	{
		/// <summary>
		/// Gets called when arma starts up and loads all extension.
		/// It's perfect to load in static objects in a seperate thread so that the extension doesn't needs any seperate initalization
		/// </summary>
		/// <param name="output">The string builder object that contains the result of the function</param>
		/// <param name="outputSize">The maximum size of bytes that can be returned</param>
#if WIN64
        [DllExport("RVExtensionVersion", CallingConvention = CallingConvention.Winapi)]
#else
		[DllImport("_RVExtensionVersion@8", CallingConvention = CallingConvention.Winapi)]
#endif
		public static void RvExtensionVersion(StringBuilder output, int outputSize)
		{

		}

		/// <summary>
		/// The entry point for the default callExtension command.
		/// </summary>
		/// <param name="output">The string builder object that contains the result of the function</param>
		/// <param name="outputSize">The maximum size of bytes that can be returned</param>
		/// <param name="function">The string argument that is used along with callExtension</param>
#if WIN64
        [DllExport("RVExtension", CallingConvention = CallingConvention.Winapi)]
#else
		[DllExport("_RVExtension@12", CallingConvention = CallingConvention.Winapi)]
#endif
		public static void RvExtension(StringBuilder output, int outputSize,
		   [MarshalAs(UnmanagedType.LPStr)] string function)
		{

		}

		/// <summary>
		/// The entry point for the callExtensionArgs command.
		/// </summary>
		/// <param name="output">The string builder object that contains the result of the function</param>
		/// <param name="outputSize">The maximum size of bytes that can be returned</param>
		/// <param name="function">The string argument that is used along with callExtension</param>
		/// <param name="args">The args passed to callExtension as a string array</param>
		/// <param name="argsCount">The size of the string array args</param>
		/// <returns>The result code</returns>
#if WIN64
        [DllExport("RVExtensionArgs", CallingConvention = CallingConvention.Winapi)]
#else
		[DllExport("_RVExtensionArgs@20", CallingConvention = CallingConvention.Winapi)]
#endif
		public static void RvExtensionArgs(StringBuilder output, int outputSize,
			[MarshalAs(UnmanagedType.LPStr)] string function,
			[MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.LPStr, SizeParamIndex = 4)] string[] args, int argCount)
		{

		}
	}
}