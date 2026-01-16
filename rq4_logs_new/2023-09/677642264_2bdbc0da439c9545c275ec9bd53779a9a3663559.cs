using System.Collections;
using System.Collections.Generic;
using Flappy.Utility;
using TMPro;
using UnityEngine;

namespace Flappy.UI
{
	/// <summary>
	/// 名前変更ポップアップウィンドウ
	/// </summary>
	/// TODO: クラス構成ややこしくなりそうなら、CommonPopupWindowの親としてBaseクラスを用意してより汎用的なモデルにする
	public class ChangeNamePopupWindow : CommonPopupWindow
	{
		[SerializeField]
		TextMeshProUGUI nameText;

		/// <summary>
		/// 初期化処理
		/// </summary>
		protected new void Awake()
		{
			// インスタンス化しただけでは表示されないように非アクティブにする
			this.gameObject.SetActive(false);

			// ボタンがあるケースしかないためFooter:有効/EmptyFooter:無効にする
			this.footer.gameObject.SetActive(true);
			this.emptyFooter.gameObject.SetActive(false);
		}

		/// <summary>
		/// 使用できないメソッドのため呼ばれたらエラー出力して処理続行
		/// </summary>
		/// <remarks>メソッドチェーン可</remarks>
		public new CommonPopupWindow SetMessage(string message)
		{
			Debug.LogAssertion("This method is not supported.");
			return this;
		}

		/// <summary>
		/// Okクリック時のアクション
		/// </summary>
		public void OnClickOk()
		{
			// TOOD
			this.Close();
		}
	}
}