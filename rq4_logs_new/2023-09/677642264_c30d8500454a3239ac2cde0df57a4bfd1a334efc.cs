#if UNITY_EDITOR
using UnityEditor;
using UnityEngine;
using UnityEngine.Events;

namespace Flappy.Editor
{
	/// <summary>
	/// エディタ上でのJsLibUtility.Promptを呼び出した時に使用される代替入力ダイアログ
	/// </summary>
	public class PromptOnEditor : EditorWindow
	{
		/// <summary>
		/// 表示するメッセージ
		/// </summary>
		private string message;

		/// <summary>
		/// ユーザの入力を保持する
		/// </summary>
		private string userInput;

		private UnityAction<string> onClickOk;

		/// <summary>
		/// 入力ウィンドウを表示する
		/// </summary>
		/// <param name="message">表示するメッセージ</param>
		/// <param name="defaultText">入力欄デフォルト値</param>
		/// <param name="onClickOk">OKクリック時のアクション</param>
		public static void ShowWindow(string message, string defaultText = "", UnityAction<string> onClickOk = null)
		{
			PromptOnEditor window = GetWindow<PromptOnEditor>("Prompt");
			window.minSize = new Vector2(250, 80);
			window.message = message;
			window.userInput = defaultText;
			window.onClickOk = onClickOk;
		}

		/// <summary>
		/// ウィンドウ内のGUIを生成
		/// </summary>
		private void OnGUI()
		{
			GUILayout.Label(this.message);
			userInput = EditorGUILayout.TextField(userInput);

			// OKボタン押下時
			if (GUILayout.Button("OK"))
			{
				// OKクリック時のアクションに入力値を返す
				this.onClickOk?.Invoke(this.userInput);

				// ウィンドウを閉じる
				Close();
			}

			// キャンセルボタン押下時
			if (GUILayout.Button("キャンセル"))
			{
				// ウィンドウを閉じる
				Close();
			}
		}
	}
}
#endif