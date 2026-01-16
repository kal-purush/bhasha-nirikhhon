using System.Collections;
using System.Collections.Generic;
using TMPro;
using UnityEngine;
using UnityEngine.UI;

namespace Flappy.UI
{
	/// <summary>
	/// 汎用ボタン
	/// </summary>
	public class CommonButton : MonoBehaviour
	{
		/// <summary>
		/// アイコン画像
		/// </summary>
		[SerializeField]
		Image icon;

		/// <summary>
		/// ボタンの文言
		/// </summary>
		[SerializeField]
		TextMeshProUGUI label;

		/// <summary>
		/// 初期化処理
		/// </summary>
		private void Start()
		{
			// アイコン画像がnullの場合はIconオブジェクトを無効にする
			this.icon.gameObject.SetActive(this.icon.sprite != null);
		}

		/// <summary>
		/// アイコン画像をセット
		/// </summary>
		/// <remarks>メソッドチェーン可</remarks>
		public CommonButton SetIcon(Sprite icon)
		{
			// アイコンの画像をセット
			this.icon.sprite = icon;

			// アイコン画像がnullの場合はIconオブジェクトを無効にする
			this.icon.gameObject.SetActive(icon != null);

			return this;
		}

		/// <summary>
		/// ボタンの文言をセット
		/// </summary>
		/// <remarks>メソッドチェーン可</remarks>
		public CommonButton SetLabel(string label)
		{
			// ボタンの文言をセット
			this.label.text = label;

			return this;
		}
	}
}