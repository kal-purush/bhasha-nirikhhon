using System.Collections;
using UnityEngine;
using UnityEngine.Events;
using DG.Tweening;

namespace Flappy.Common
{
	/// <summary>
	/// フルスクリーンロード画面ベースクラス
	/// </summary>
	public abstract class FullscreenLoadingBase : LoadingBase
	{
		/// <summary>
		/// CanvasGroupコンポーネント
		/// </summary>
		[SerializeField]
		protected CanvasGroup canvasGroup;

		/// <summary>
		/// フェードイン・アウトに掛ける時間(秒)
		/// </summary>
		protected abstract float fadeTime { get; }

		/// <summary>
		/// 最低表示時間
		/// </summary>
		protected abstract float minDisplayTime { get; }

		/// <summary>
		/// 経過時間
		/// </summary>
		protected float elapsedTime = 0f;

		/// <summary>
		/// 更新
		/// </summary>
		protected virtual void Update()
		{
			this.elapsedTime += Time.deltaTime;
		}

		/// <summary>
		/// ロード画面を表示する
		/// </summary>
		/// <param name="onBeginLoad">ロード開始時(フェードイン完了後)に実行するアクション</param>
		/// <param name="onCompleteLoad">ロード完了時(フェードイン開始前)に実行するアクション</param>
		public override void Show(UnityAction onBeginLoad = null, UnityAction onCompleteLoad = null)
		{
			// ロード開始・完了時のアクションをセット
			this.onBeginLoad = onBeginLoad;
			this.onCompleteLoad = onCompleteLoad;

			// ゲームオブジェクトを有効にする
			this.gameObject.SetActive(true);

			// フェードイン処理
			this.canvasGroup.alpha = 0f;
			this.canvasGroup.DOFade(1f, this.fadeTime).OnComplete(() =>
			{
				// ロード開始時アクションを実行する
				this.onBeginLoad?.Invoke();
			});
		}

		/// <summary>
		/// ロード画面を非表示にする
		/// </summary>
		protected override void Hide()
		{
			// 最低表示時間が経過するまで待機
			this.StartCoroutine(this.WaitForMinDisplayTime(() =>
			{
				// ロード完了時アクションを実行する
				this.onCompleteLoad?.Invoke();

				// フェードアウト処理
				this.canvasGroup.alpha = 1f;
				this.canvasGroup.DOFade(0f, fadeTime).OnComplete(() =>
				{
					// ゲームオブジェクトを削除する
					GameObject.Destroy(this.gameObject);
				});
			}));
		}

		/// <summary>
		/// 最低表示時間が経過するまで待機するコルーチン
		/// </summary>
		/// <param name="completed">待機後に実行するアクション</param>
		private IEnumerator WaitForMinDisplayTime(UnityAction completed)
		{
			// 最低表示時間が経過するまで待機 (フェードイン時間分は必ず待つ)
			while (this.elapsedTime < Mathf.Max(this.minDisplayTime, this.fadeTime))
			{
				yield return null;
			}
			completed?.Invoke();
		}
	}
}