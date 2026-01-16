using System.Collections;
using System.Collections.Generic;
using Flappy.Utility;
using TMPro;
using UnityEngine;

namespace Flappy.UI
{
	public class CommonInput : MonoBehaviour
	{
		/// <summary>
		/// Textオブジェクト
		/// </summary>
		[SerializeField]
		private TextMeshProUGUI textMeshPro;

		/// <summary>
		/// 最大文字数
		/// </summary>
		/// <remarks>-1で無制限</remarks>
		public int maxLength = -1;

		/// <summary>
		/// 入力値
		/// </summary>
		public string Value
		{
			get
			{
				return this.textMeshPro.text;
			}
			set
			{
				this.textMeshPro.text = value;
			}
		}

		/// <summary>
		/// 入力プロンプトを開く
		/// </summary>
		/// <param name="message">プロンプトに表示するメッセージ</param>
		public void OpenPrompt(string message)
		{
			JsLibUtility.Prompt(message, this.textMeshPro.text, this.ChangeValue);
		}

		/// <summary>
		/// 値を変更
		/// </summary>
		/// <param name="newValue">変更後の値</param>
		private void ChangeValue(string newValue)
		{
			// 最大文字数を超える場合は切り取る
			if( this.maxLength != -1 && newValue.Length > this.maxLength )
			{
				newValue = newValue.Substring(0, maxLength);
			}

			this.Value = newValue;
		}
	}
}