using System.Runtime.InteropServices;

namespace Flappy.Utility
{
	public class JsLibUtility
	{
		/// <summary>
		/// flappy.jslib -> Prompt
		/// </summary>
		/// <returns></returns>
		[DllImport("__Internal")]
		private static extern string PromptInternal(string message, string defaultText);

		/// <summary>
		/// 文字入力欄を表示する
		/// </summary>
		/// <param name="message">表示するメッセージ</param>
		/// <param name="defaultText">入力欄の初期値</param>
		/// <returns>入力がキャンセルされた、または何も入力されずOKを押下された場合はnullが返る</returns>
		public static string Prompt(string message, string defaultText = "")
		{
			// キャンセルされた時にjslibから返る文字列
			const string canceledValue = "%canceled%";

			// キャンセルされた場合は null を返す
			var result = JsLibUtility.PromptInternal(message, defaultText);
			if( result == canceledValue )
			{
				return null;
			}

			// 入力された値を返す
			return result;
		}
	}
}
