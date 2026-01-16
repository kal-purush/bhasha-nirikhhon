using TMPro;
using UnityEngine;
using DG.Tweening;

namespace Flappy.UI
{
	/// <summary>
	/// シンプルウィンドウ
	/// </summary>
	/// <remarks>必ず Show() を呼ぶこと。そうしないと表示されない。</remarks>
	public class SimplePopupWindow : MonoBehaviour
	{
		/// <summary>
		/// 表示時間 (秒)
		/// </summary>
		const float defaultDisplayTime = 3f;

		/// <summary>
		/// フェードイン/アウトの時間 (秒)
		/// </summary>
		const float fadeTime = 0.1f;

		/// <summary>
		/// 拡大縮小の最小サイズ (倍率)
		/// </summary>
		const float shrinkSize = 0.9f;

		/// <summary>
		/// メッセージ文言
		/// </summary>
		[SerializeField]
		TextMeshProUGUI message;

		/// <summary>
		/// CanvasGroupコンポーネント
		/// </summary>
		[SerializeField]
		CanvasGroup canvasGroup;

		/// <summary>
		/// WindowSizeCapオブジェクト
		/// </summary>
		[SerializeField]
		GameObject windowSizeCap;

		/// <summary>
		/// 初期化処理
		/// </summary>
		private void Awake()
		{
			// インスタンス化しただけでは表示されないように非アクティブにする
			this.gameObject.SetActive(false);
		}

		/// <summary>
		/// メッセージ文言をセット
		/// </summary>
		/// <remarks>メソッドチェーン可</remarks>
		public SimplePopupWindow SetMessage(string message)
		{
			this.message.text = message;
			return this;
		}

		/// <summary>
		/// ウィンドウを表示する
		/// </summary>
		/// <param name="displayTime">表示する時間 (秒)</param>
		/// <param name="isDestroyAfterDisplay">表示後にオブジェクトを破壊するか</param>
		public void Show(float displayTime = SimplePopupWindow.defaultDisplayTime, bool isDestroyAfterDisplay = true)
		{
			// ゲームオブジェクトをアクティブにする
			this.gameObject.SetActive(true);

			// 拡大処理
			this.windowSizeCap.transform.localScale = Vector2.one * SimplePopupWindow.shrinkSize;
			this.windowSizeCap.transform.DOScale(1f, SimplePopupWindow.fadeTime);

			// フェードイン処理
			this.canvasGroup.alpha = 0f;
			this.canvasGroup.DOFade(1f, SimplePopupWindow.fadeTime).OnComplete(() =>
			{
				// 指定時間経過後に実行
				DOVirtual.DelayedCall(displayTime, () =>
				{
					// ウィンドウを閉じる
					this.Close(isDestroyAfterDisplay);
				});
			});
		}

		/// <summary>
		/// ウィンドウを閉じる
		/// </summary>
		/// <param name="isDestroyAfterDisplay">オブジェクトを破壊するか</param>
		private void Close(bool isDestroy)
		{
			// 縮小処理
			this.windowSizeCap.transform.DOScale(SimplePopupWindow.shrinkSize, SimplePopupWindow.fadeTime);

			// フェードアウト処理
			this.canvasGroup.DOFade(0f, SimplePopupWindow.fadeTime).OnComplete(() =>
			{
				if (isDestroy == true)
				{
					GameObject.Destroy(this.gameObject);
				}
			});
		}
	}
}
